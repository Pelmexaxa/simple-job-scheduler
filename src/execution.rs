//! Выполнение пайплайна задачи: HTTP fetch → JS-преобразование → HTTP send, с повторами.

use crate::database::DbPool;
use crate::i18n::{LogLang, LogMsg};
use crate::jobs;
use crate::models::{now_rfc3339, ExecutionLogRow, JobRow};
use crate::scheduler::schedule_after_success;
use boa_engine::{Context, JsValue, Source};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Максимальный размер тела HTTP-ответа (10 МБ).
const MAX_RESPONSE_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Ошибка одной попытки пайплайна с частичными метриками для журнала.
struct AttemptFailure {
    message: String,
    fetch_status: Option<i32>,
    send_status: Option<i32>,
    preview: Option<String>,
    preview_truncated: bool,
}

/// Контекст одного выполнения: пул БД, таймауты, флаг JS и язык логов.
pub struct ExecutionContext {
    pub pool: DbPool,
    pub http_timeout_secs: u64,
    pub enable_js: bool,
    pub log_lang: LogLang,
    pub preview_max_bytes: usize,
    pub cancel: watch::Receiver<bool>,
}

fn shutdown_requested(cancel: &watch::Receiver<bool>) -> bool {
    *cancel.borrow()
}

fn shutdown_error(lang: LogLang) -> String {
    LogMsg::JobCancelled.text(lang).to_string()
}

async fn wait_for_shutdown(cancel: &mut watch::Receiver<bool>) {
    if *cancel.borrow() {
        return;
    }
    let _ = cancel.changed().await;
}

/// Выполняет полный цикл задачи с повторами, записывает журнал и обновляет расписание.
pub async fn run_job(ctx: &ExecutionContext, job: &JobRow) -> Result<(), String> {
    let lang = ctx.log_lang;
    let started = Instant::now();
    let log_id = Uuid::new_v4().to_string();
    let started_at = now_rfc3339();

    info!(
        job_id = %job.id,
        name = %job.name,
        "{}", LogMsg::JobExecutionStart.text(lang)
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(ctx.http_timeout_secs))
        .build()
        .map_err(|e| e.to_string())?;

    let max_retries = if job.retry_enabled != 0 {
        job.max_retries.unwrap_or(0) as u32
    } else {
        0
    };
    let retry_interval = job.retry_interval_seconds.unwrap_or(60) as u64;

    let mut attempt = 0u32;
    let mut last_error: Option<String> = None;
    let mut fetch_status: Option<i32> = None;
    let mut send_status: Option<i32> = None;
    let mut preview: Option<String> = None;
    let mut preview_truncated = false;
    let mut success = false;

    loop {
        if shutdown_requested(&ctx.cancel) {
            warn!(
                job_id = %job.id,
                name = %job.name,
                "{}", LogMsg::JobCancelled.text(lang)
            );
            return Err(shutdown_error(lang));
        }

        match execute_once(ctx, job, &client).await {
            Ok(result) => {
                fetch_status = result.fetch_status;
                send_status = result.send_status;
                preview = result.preview;
                preview_truncated = result.preview_truncated;
                success = true;
                break;
            }
            Err(failure) => {
                last_error = Some(failure.message.clone());
                if failure.fetch_status.is_some() {
                    fetch_status = failure.fetch_status;
                }
                if failure.send_status.is_some() {
                    send_status = failure.send_status;
                }
                if failure.preview.is_some() {
                    preview = failure.preview;
                }
                if failure.preview_truncated {
                    preview_truncated = true;
                }
                error!(
                    job_id = %job.id,
                    attempt = attempt,
                    max_retries = max_retries,
                    error = %failure.message,
                    fetch_status = ?failure.fetch_status,
                    send_status = ?failure.send_status,
                    "{}", LogMsg::PipelineStepFailed.text(lang)
                );
                if attempt >= max_retries {
                    break;
                }
                attempt += 1;
                warn!(
                    job_id = %job.id,
                    attempt = attempt,
                    max_retries = max_retries,
                    error = %failure.message,
                    "{}", LogMsg::JobRetryAttempt.text(lang)
                );
                let mut cancel = ctx.cancel.clone();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(retry_interval)) => {}
                    _ = wait_for_shutdown(&mut cancel) => {
                        warn!(
                            job_id = %job.id,
                            name = %job.name,
                            "{}", LogMsg::JobCancelled.text(lang)
                        );
                        return Err(shutdown_error(lang));
                    }
                }
            }
        }
    }

    let finished_at = now_rfc3339();
    let duration_ms = started.elapsed().as_millis() as i64;
    let status = if success { "succeeded" } else { "failed" };

    let log = ExecutionLogRow {
        id: log_id,
        job_id: job.id.clone(),
        started_at,
        finished_at: Some(finished_at.clone()),
        status: status.to_string(),
        fetch_status: fetch_status.map(i64::from),
        send_status: send_status.map(i64::from),
        duration_ms: Some(duration_ms),
        error_message: last_error.clone(),
        response_preview: preview,
        preview_truncated: if preview_truncated { 1 } else { 0 },
    };

    jobs::insert_log(&ctx.pool, &log)
        .await
        .map_err(|e| e.to_string())?;

    let last_run_dt = chrono::Utc::now();
    let next = schedule_after_success(&job.schedule_type, &job.schedule_value, last_run_dt);

    jobs::update_run_times(&ctx.pool, &job.id, &finished_at, next)
        .await
        .map_err(|e| e.to_string())?;

    if success {
        info!(
            job_id = %job.id,
            name = %job.name,
            duration_ms = duration_ms,
            fetch_status = ?fetch_status,
            send_status = ?send_status,
            "{}", LogMsg::JobExecutionSuccess.text(lang)
        );
        Ok(())
    } else {
        warn!(
            job_id = %job.id,
            name = %job.name,
            duration_ms = duration_ms,
            error = last_error.as_deref().unwrap_or(""),
            "{}", LogMsg::JobExecutionFailed.text(lang)
        );
        Err(LogMsg::JobExecutionFailed.text(lang).to_string())
    }
}

/// Результат одной попытки выполнения шагов fetch/send.
struct StepResult {
    fetch_status: Option<i32>,
    send_status: Option<i32>,
    preview: Option<String>,
    preview_truncated: bool,
}

/// Один проход пайплайна без учёта повторов.
async fn execute_once(
    ctx: &ExecutionContext,
    job: &JobRow,
    client: &Client,
) -> Result<StepResult, AttemptFailure> {
    let lang = ctx.log_lang;
    let mut payload: Option<String> = None;
    let mut fetch_status = None;
    let mut send_status = None;
    let mut data;

    if job.fetch_enabled != 0 {
        let method = job.fetch_method.as_deref().unwrap_or("GET");
        let url = non_empty_url(job.fetch_url.as_deref()).ok_or_else(|| AttemptFailure {
            message: step_error_simple(lang, "fetch", "не указан URL"),
            fetch_status: None,
            send_status: None,
            preview: None,
            preview_truncated: false,
        })?;
        debug!(
            job_id = %job.id,
            method = method,
            url = url,
            "{}", LogMsg::StepFetch.text(lang)
        );
        let headers = parse_headers(job.fetch_headers.as_deref()).map_err(|e| AttemptFailure {
            message: step_error_simple(lang, "fetch", &e),
            fetch_status: None,
            send_status: None,
            preview: None,
            preview_truncated: false,
        })?;
        match http_request(
            client,
            method,
            url,
            &headers,
            job.fetch_body.as_deref(),
            "fetch",
            lang,
            &ctx.cancel,
        )
        .await
        {
            Ok((status, body)) => {
                fetch_status = Some(status);
                if !(200..300).contains(&status) {
                    let (preview, preview_truncated) = preview_for(ctx, &body);
                    return Err(AttemptFailure {
                        message: http_status_error(lang, "fetch", url, status, &body),
                        fetch_status: Some(status),
                        send_status: None,
                        preview,
                        preview_truncated,
                    });
                }
                debug!(
                    job_id = %job.id,
                    status = status,
                    body_len = body.len(),
                    "{}", LogMsg::StepFetchDone.text(lang)
                );
                payload = Some(body);
            }
            Err((status_opt, msg)) => {
                return Err(AttemptFailure {
                    message: msg,
                    fetch_status: status_opt,
                    send_status: None,
                    preview: None,
                    preview_truncated: false,
                });
            }
        }
    }

    data = payload.unwrap_or_else(|| "{}".to_string());

    if job.transform_enabled != 0 && ctx.enable_js {
        let script = job
            .transform_script
            .as_deref()
            .ok_or_else(|| {
                let (preview, preview_truncated) = preview_for(ctx, &data);
                AttemptFailure {
                    message: step_error_simple(lang, "transform", "не указан скрипт"),
                    fetch_status,
                    send_status: None,
                    preview,
                    preview_truncated,
                }
            })?;
        debug!(job_id = %job.id, "{}", LogMsg::StepTransform.text(lang));
        data = run_transform(script, &data).map_err(|e| {
            let (preview, preview_truncated) = preview_for(ctx, &data);
            AttemptFailure {
                message: step_error_simple(lang, "transform", &e),
                fetch_status,
                send_status: None,
                preview,
                preview_truncated,
            }
        })?;
        debug!(
            job_id = %job.id,
            result_len = data.len(),
            "{}", LogMsg::StepTransformDone.text(lang)
        );
    }

    if job.send_enabled != 0 {
        let method = job.send_method.as_deref().unwrap_or("POST");
        let url = non_empty_url(job.send_url.as_deref()).ok_or_else(|| {
            let (preview, preview_truncated) = preview_for(ctx, &data);
            AttemptFailure {
                message: step_error_simple(lang, "send", "не указан URL"),
                fetch_status,
                send_status: None,
                preview,
                preview_truncated,
            }
        })?;
        debug!(
            job_id = %job.id,
            method = method,
            url = url,
            "{}", LogMsg::StepSend.text(lang)
        );
        let mut headers = parse_headers(job.send_headers.as_deref()).map_err(|e| {
            let (preview, preview_truncated) = preview_for(ctx, &data);
            AttemptFailure {
                message: step_error_simple(lang, "send", &e),
                fetch_status,
                send_status: None,
                preview,
                preview_truncated,
            }
        })?;
        let body = if let Some(template) = &job.send_body_template {
            if template.contains("{{payload}}") {
                template.replace("{{payload}}", &data)
            } else {
                template.clone()
            }
        } else {
            data.clone()
        };

        if !headers.contains_key(CONTENT_TYPE) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        match http_request(
            client,
            method,
            url,
            &headers,
            Some(&body),
            "send",
            lang,
            &ctx.cancel,
        )
        .await
        {
            Ok((status, resp_body)) => {
                send_status = Some(status);
                if !(200..300).contains(&status) {
                    let (preview, preview_truncated) = preview_for(ctx, &data);
                    return Err(AttemptFailure {
                        message: http_status_error(lang, "send", url, status, &resp_body),
                        fetch_status,
                        send_status: Some(status),
                        preview,
                        preview_truncated,
                    });
                }
                debug!(
                    job_id = %job.id,
                    status = status,
                    "{}", LogMsg::StepSendDone.text(lang)
                );
            }
            Err((status_opt, msg)) => {
                let (preview, preview_truncated) = preview_for(ctx, &data);
                return Err(AttemptFailure {
                    message: msg,
                    fetch_status,
                    send_status: status_opt,
                    preview,
                    preview_truncated,
                });
            }
        }
    }

    let (preview_text, preview_truncated) = truncate_preview(&data, ctx.preview_max_bytes);

    Ok(StepResult {
        fetch_status,
        send_status,
        preview: Some(preview_text),
        preview_truncated,
    })
}

/// Выполняет HTTP-запрос и возвращает код ответа и тело (UTF-8).
async fn http_request(
    client: &Client,
    method: &str,
    url: &str,
    headers: &HeaderMap,
    body: Option<&str>,
    step: &str,
    lang: LogLang,
    cancel: &watch::Receiver<bool>,
) -> Result<(i32, String), (Option<i32>, String)> {
    let method = method.to_uppercase();
    debug!(
        method = %method,
        url = url,
        step = step,
        has_body = body.is_some_and(|b| !b.is_empty()),
        "{}", LogMsg::HttpOutbound.text(lang)
    );

    let mut req = match method.as_str() {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        _ => {
            return Err((
                None,
                step_error_simple(lang, step, &format!("неподдерживаемый HTTP-метод: {method}")),
            ))
        }
    };

    req = req.headers(headers.clone());
    if let Some(b) = body {
        if !b.is_empty() {
            req = req.body(b.to_string());
        }
    }

    if shutdown_requested(cancel) {
        return Err((None, shutdown_error(lang)));
    }

    let mut cancel_rx = cancel.clone();
    let resp = tokio::select! {
        res = req.send() => res.map_err(|e| {
            (
                None,
                step_error_simple(lang, step, &format!("сеть: {e}")),
            )
        })?,
        _ = wait_for_shutdown(&mut cancel_rx) => {
            return Err((None, shutdown_error(lang)));
        }
    };

    read_response_body(resp, step, url, lang)
        .await
        .map_err(|(status, msg)| (Some(status), msg))
}

/// Читает тело ответа как байты и проверяет UTF-8 (вместо `text()`, дающего «error decoding response body»).
async fn read_response_body(
    resp: reqwest::Response,
    step: &str,
    url: &str,
    lang: LogLang,
) -> Result<(i32, String), (i32, String)> {
    let status = resp.status().as_u16() as i32;
    let content_type = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("—")
        .to_string();
    let content_length = resp.content_length();

    let bytes = resp.bytes().await.map_err(|e| {
        (
            status,
            http_body_error(
                lang,
                step,
                url,
                Some(status),
                &content_type,
                content_length,
                0,
                match lang {
                    LogLang::Ru => "чтение_тела",
                    LogLang::En => "read_body",
                },
                &e.to_string(),
            ),
        )
    })?;

    let byte_len = bytes.len();
    if byte_len > MAX_RESPONSE_BODY_BYTES {
        let size_detail = match lang {
            LogLang::Ru => format!("тело больше лимита {MAX_RESPONSE_BODY_BYTES} байт"),
            LogLang::En => format!("body exceeds limit of {MAX_RESPONSE_BODY_BYTES} bytes"),
        };
        return Err((
            status,
            http_body_error(
                lang,
                step,
                url,
                Some(status),
                &content_type,
                content_length,
                byte_len,
                match lang {
                    LogLang::Ru => "размер",
                    LogLang::En => "size",
                },
                &size_detail,
            ),
        ));
    }

    match std::str::from_utf8(&bytes) {
        Ok(text) => Ok((status, text.to_string())),
        Err(e) => {
            let valid = e.valid_up_to();
            let snippet = body_snippet(&bytes);
            let detail = match lang {
                LogLang::Ru => format!(
                    "тело не UTF-8 (валидно {valid}/{byte_len} байт); начало: {snippet}"
                ),
                LogLang::En => format!(
                    "body is not valid UTF-8 (valid {valid}/{byte_len} bytes); start: {snippet}"
                ),
            };
            Err((
                status,
                http_body_error(
                    lang,
                    step,
                    url,
                    Some(status),
                    &content_type,
                    content_length,
                    byte_len,
                    match lang {
                        LogLang::Ru => "кодировка",
                        LogLang::En => "encoding",
                    },
                    &detail,
                ),
            ))
        }
    }
}

fn http_status_error(lang: LogLang, step: &str, url: &str, status: i32, body: &str) -> String {
    let preview = truncate(body, 200);
    match lang {
        LogLang::Ru => format!(
            "шаг={step}; url={url}; HTTP={status}; ответ: {preview}"
        ),
        LogLang::En => format!(
            "step={step}; url={url}; HTTP={status}; response: {preview}"
        ),
    }
}

fn step_error_simple(lang: LogLang, step: &str, detail: &str) -> String {
    match lang {
        LogLang::Ru => format!("шаг={step}; {detail}"),
        LogLang::En => format!("step={step}; {detail}"),
    }
}

fn http_body_error(
    lang: LogLang,
    step: &str,
    url: &str,
    status: Option<i32>,
    content_type: &str,
    content_length: Option<u64>,
    byte_len: usize,
    kind: &str,
    detail: &str,
) -> String {
    let status_s = status
        .map(|s| s.to_string())
        .unwrap_or_else(|| match lang {
            LogLang::Ru => "—".into(),
            LogLang::En => "—".into(),
        });
    let len_hint = content_length
        .map(|n| n.to_string())
        .unwrap_or_else(|| match lang {
            LogLang::Ru => "неизвестно".into(),
            LogLang::En => "unknown".into(),
        });
    match lang {
        LogLang::Ru => format!(
            "шаг={step}; url={url}; HTTP={status_s}; Content-Type={content_type}; \
             Content-Length={len_hint}; прочитано={byte_len} байт; {kind}: {detail}"
        ),
        LogLang::En => format!(
            "step={step}; url={url}; HTTP={status_s}; Content-Type={content_type}; \
             Content-Length={len_hint}; read={byte_len} bytes; {kind}: {detail}"
        ),
    }
}

/// Короткое превью бинарного или битого тела для журнала.
fn body_snippet(bytes: &[u8]) -> String {
    const MAX: usize = 80;
    let slice = &bytes[..bytes.len().min(MAX)];
    let lossy = String::from_utf8_lossy(slice);
    if bytes.len() > MAX {
        format!("{lossy}…")
    } else {
        lossy.into_owned()
    }
}

/// Разбирает заголовки из JSON-объекта (`{"Authorization": "Bearer …"}`).
fn parse_headers(raw: Option<&str>) -> Result<HeaderMap, String> {
    let mut map = HeaderMap::new();
    let Some(raw) = raw else {
        return Ok(map);
    };

    if raw.trim().is_empty() {
        return Ok(map);
    }

    let value: Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    let obj = value
        .as_object()
        .ok_or_else(|| "заголовки должны быть JSON-объектом".to_string())?;

    for (k, v) in obj {
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| e.to_string())?;
        let val_str = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let hv = HeaderValue::from_str(&val_str).map_err(|e| e.to_string())?;
        if k.eq_ignore_ascii_case("authorization") {
            map.insert(AUTHORIZATION, hv);
        } else {
            map.insert(name, hv);
        }
    }

    Ok(map)
}

/// Выполняет JS-скрипт в песочнице boa: переменная `input`, результат — JSON-строка.
pub fn run_transform(script: &str, input_json: &str) -> Result<String, String> {
    let input_value: Value =
        serde_json::from_str(input_json).unwrap_or(Value::String(input_json.to_string()));
    let input_str = serde_json::to_string(&input_value).map_err(|e| e.to_string())?;

    let wrapped = format!(
        "(function() {{
            const input = {input_str};
            {script}
        }})()"
    );

    let mut context = Context::default();
    let result = context
        .eval(Source::from_bytes(wrapped.as_bytes()))
        .map_err(|e| format!("ошибка JS-преобразования: {e}"))?;

    js_value_to_json_string(&result, &mut context)
}

/// Сериализует результат JS через `JSON.stringify`.
fn js_value_to_json_string(value: &JsValue, context: &mut Context) -> Result<String, String> {
    if value.is_undefined() || value.is_null() {
        return Ok("null".to_string());
    }
    let json_fn = context
        .eval(Source::from_bytes(b"JSON.stringify"))
        .map_err(|e| e.to_string())?;
    let json_fn = json_fn
        .as_callable()
        .ok_or_else(|| "JSON.stringify недоступен".to_string())?;
    let out = json_fn
        .call(&JsValue::undefined(), &[value.clone()], context)
        .map_err(|e| format!("ошибка JSON.stringify: {e}"))?;
    out.as_string()
        .map(|s| s.to_std_string_escaped())
        .ok_or_else(|| "результат преобразования не сериализуется в JSON".to_string())
}

/// Возвращает URL, если строка не пустая после trim.
fn non_empty_url(url: Option<&str>) -> Option<&str> {
    url.and_then(|s| {
        let t = s.trim();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
    })
}

/// Обрезает строку для превью в журнале; возвращает текст и флаг обрезки.
fn truncate_preview(s: &str, max: usize) -> (String, bool) {
    if s.len() <= max {
        return (s.to_string(), false);
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (format!("{}…", &s[..end]), true)
}

/// Короткий фрагмент для встраивания в текст ошибки HTTP.
fn truncate(s: &str, max: usize) -> String {
    truncate_preview(s, max).0
}

fn preview_for(ctx: &ExecutionContext, s: &str) -> (Option<String>, bool) {
    let (text, truncated) = truncate_preview(s, ctx.preview_max_bytes);
    (Some(text), truncated)
}

#[cfg(test)]
mod tests {
    use super::run_transform;

    #[test]
    fn transform_maps_users() {
        let input = r#"{"users":[{"name":"John"}]}"#;
        let script = "return input.users.map(x => ({ username: x.name }));";
        let out = run_transform(script, input).unwrap();
        assert!(out.contains("username"));
    }
}
