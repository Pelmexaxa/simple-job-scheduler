//! Выполнение пайплайна задачи: упорядоченные шаги http / transform / command.

use crate::database::DbPool;
use crate::i18n::{LogLang, LogMsg};
use crate::jobs;
use crate::models::{now_rfc3339, ExecutionLogRow, JobRow, JobStep, StepKind, StepLogEntry};
use crate::scheduler::schedule_after_success;
use boa_engine::{Context, JsValue, Source};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde_json::Value;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Максимальный размер тела HTTP-ответа / вывода команды (10 МБ).
pub const MAX_STEP_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// Ошибка одной попытки пайплайна с частичными метриками для журнала.
struct AttemptFailure {
    message: String,
    fetch_status: Option<i32>,
    send_status: Option<i32>,
    output: Option<String>,
    steps_log: Vec<StepLogEntry>,
}

/// Контекст одного выполнения: пул БД, таймауты, флаг JS и язык логов.
pub struct ExecutionContext {
    pub pool: DbPool,
    pub http_timeout_secs: u64,
    pub enable_js: bool,
    pub log_lang: LogLang,
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
    let mut output: Option<String> = None;
    let mut steps_log: Vec<StepLogEntry> = Vec::new();
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
                output = result.output;
                steps_log = result.steps_log;
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
                if failure.output.is_some() {
                    output = failure.output;
                }
                if !failure.steps_log.is_empty() {
                    steps_log = failure.steps_log;
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
    let steps_log_json = serde_json::to_string(&steps_log).ok();

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
        response_preview: output,
        preview_truncated: 0,
        steps_log: steps_log_json,
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

/// Результат одной попытки выполнения шагов.
struct StepResult {
    fetch_status: Option<i32>,
    send_status: Option<i32>,
    output: Option<String>,
    steps_log: Vec<StepLogEntry>,
}

/// Один проход пайплайна без учёта повторов.
async fn execute_once(
    ctx: &ExecutionContext,
    job: &JobRow,
    client: &Client,
) -> Result<StepResult, AttemptFailure> {
    let lang = ctx.log_lang;
    let steps = job.parsed_steps();
    let mut payload = "{}".to_string();
    let mut steps_log: Vec<StepLogEntry> = Vec::new();
    let mut first_http_status: Option<i32> = None;
    let mut last_http_status: Option<i32> = None;

    for step in &steps {
        if shutdown_requested(&ctx.cancel) {
            return Err(AttemptFailure {
                message: shutdown_error(lang),
                fetch_status: first_http_status,
                send_status: last_http_status,
                output: Some(payload),
                steps_log,
            });
        }

        match step.kind {
            StepKind::Http => {
                run_http_step(
                    ctx,
                    job,
                    client,
                    step,
                    &mut payload,
                    &mut steps_log,
                    &mut first_http_status,
                    &mut last_http_status,
                )
                .await?;
            }
            StepKind::Transform => {
                run_transform_step(ctx, job, step, &mut payload, &mut steps_log, first_http_status, last_http_status)?;
            }
            StepKind::Command => {
                run_command_step(
                    ctx,
                    job,
                    step,
                    &mut payload,
                    &mut steps_log,
                    first_http_status,
                    last_http_status,
                )
                .await?;
            }
        }
    }

    Ok(StepResult {
        fetch_status: first_http_status,
        send_status: last_http_status,
        output: Some(payload),
        steps_log,
    })
}

async fn run_http_step(
    ctx: &ExecutionContext,
    job: &JobRow,
    client: &Client,
    step: &JobStep,
    payload: &mut String,
    steps_log: &mut Vec<StepLogEntry>,
    first_http_status: &mut Option<i32>,
    last_http_status: &mut Option<i32>,
) -> Result<(), AttemptFailure> {
    let lang = ctx.log_lang;
    let step_label = step_label(step);
    let method = step.method.as_deref().unwrap_or("GET");
    let url = non_empty_url(step.url.as_deref()).ok_or_else(|| {
        fail_step(
            lang,
            &step_label,
            "не указан URL",
            *first_http_status,
            *last_http_status,
            payload,
            steps_log,
            step,
            None,
            None,
            None,
        )
    })?;

    debug!(
        job_id = %job.id,
        method = method,
        url = url,
        step_id = %step.id,
        "{}", LogMsg::StepHttp.text(lang)
    );

    let headers = parse_headers(step.headers.as_deref()).map_err(|e| {
        fail_step(
            lang,
            &step_label,
            &e,
            *first_http_status,
            *last_http_status,
            payload,
            steps_log,
            step,
            None,
            None,
            None,
        )
    })?;

    let body = resolve_http_body(step, payload);

    match http_request(
        client,
        method,
        url,
        &headers,
        body.as_deref(),
        &step_label,
        lang,
        &ctx.cancel,
    )
    .await
    {
        Ok((status, body)) => {
            if first_http_status.is_none() {
                *first_http_status = Some(status);
            }
            *last_http_status = Some(status);

            if !(200..300).contains(&status) {
                steps_log.push(StepLogEntry {
                    id: step.id.clone(),
                    kind: StepKind::Http,
                    name: step.name.clone(),
                    http_status: Some(status),
                    exit_code: None,
                    output: Some(body.clone()),
                    error: Some(http_status_error(lang, &step_label, url, status, &body)),
                });
                return Err(AttemptFailure {
                    message: http_status_error(lang, &step_label, url, status, &body),
                    fetch_status: *first_http_status,
                    send_status: *last_http_status,
                    output: Some(payload.clone()),
                    steps_log: steps_log.clone(),
                });
            }

            debug!(
                job_id = %job.id,
                status = status,
                body_len = body.len(),
                "{}", LogMsg::StepHttpDone.text(lang)
            );

            steps_log.push(StepLogEntry {
                id: step.id.clone(),
                kind: StepKind::Http,
                name: step.name.clone(),
                http_status: Some(status),
                exit_code: None,
                output: Some(body.clone()),
                error: None,
            });

            if step.capture_output {
                *payload = body;
            }
            Ok(())
        }
        Err((status_opt, msg)) => {
            if let Some(status) = status_opt {
                if first_http_status.is_none() {
                    *first_http_status = Some(status);
                }
                *last_http_status = Some(status);
            }
            steps_log.push(StepLogEntry {
                id: step.id.clone(),
                kind: StepKind::Http,
                name: step.name.clone(),
                http_status: status_opt,
                exit_code: None,
                output: None,
                error: Some(msg.clone()),
            });
            Err(AttemptFailure {
                message: msg,
                fetch_status: *first_http_status,
                send_status: *last_http_status,
                output: Some(payload.clone()),
                steps_log: steps_log.clone(),
            })
        }
    }
}

fn resolve_http_body(step: &JobStep, payload: &str) -> Option<String> {
    if step.body_from_payload {
        return Some(payload.to_string());
    }
    if let Some(template) = &step.body {
        if template.contains("{{payload}}") {
            return Some(template.replace("{{payload}}", payload));
        }
        if !template.trim().is_empty() {
            return Some(template.clone());
        }
    }
    None
}

fn run_transform_step(
    ctx: &ExecutionContext,
    job: &JobRow,
    step: &JobStep,
    payload: &mut String,
    steps_log: &mut Vec<StepLogEntry>,
    first_http_status: Option<i32>,
    last_http_status: Option<i32>,
) -> Result<(), AttemptFailure> {
    let lang = ctx.log_lang;
    let step_label = step_label(step);

    if !ctx.enable_js {
        debug!(job_id = %job.id, "{}", LogMsg::StepTransformSkipped.text(lang));
        steps_log.push(StepLogEntry {
            id: step.id.clone(),
            kind: StepKind::Transform,
            name: step.name.clone(),
            http_status: None,
            exit_code: None,
            output: Some(payload.clone()),
            error: None,
        });
        return Ok(());
    }

    let script = step.script.as_deref().filter(|s| !s.trim().is_empty()).ok_or_else(|| {
        fail_step(
            lang,
            &step_label,
            "не указан скрипт",
            first_http_status,
            last_http_status,
            payload,
            steps_log,
            step,
            None,
            None,
            Some(payload.clone()),
        )
    })?;

    debug!(job_id = %job.id, "{}", LogMsg::StepTransform.text(lang));
    let out = run_transform(script, payload).map_err(|e| {
        fail_step(
            lang,
            &step_label,
            &e,
            first_http_status,
            last_http_status,
            payload,
            steps_log,
            step,
            None,
            None,
            Some(payload.clone()),
        )
    })?;

    debug!(
        job_id = %job.id,
        result_len = out.len(),
        "{}", LogMsg::StepTransformDone.text(lang)
    );

    steps_log.push(StepLogEntry {
        id: step.id.clone(),
        kind: StepKind::Transform,
        name: step.name.clone(),
        http_status: None,
        exit_code: None,
        output: Some(out.clone()),
        error: None,
    });

    if step.capture_output {
        *payload = out;
    }
    Ok(())
}

async fn run_command_step(
    ctx: &ExecutionContext,
    job: &JobRow,
    step: &JobStep,
    payload: &mut String,
    steps_log: &mut Vec<StepLogEntry>,
    first_http_status: Option<i32>,
    last_http_status: Option<i32>,
) -> Result<(), AttemptFailure> {
    let lang = ctx.log_lang;
    let step_label = step_label(step);
    let program = step
        .program
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            fail_step(
                lang,
                &step_label,
                "не указана программа",
                first_http_status,
                last_http_status,
                payload,
                steps_log,
                step,
                None,
                None,
                None,
            )
        })?;

    // ponytail: UI/пользователь часто пишет `-t processor` одним токеном — режем по whitespace
    let args = flatten_command_args(&step.args);

    debug!(
        job_id = %job.id,
        program = program,
        args = ?args,
        "{}", LogMsg::StepCommand.text(lang)
    );

    let mut cmd = Command::new(program);
    cmd.args(&args);
    if let Some(cwd) = step.cwd.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        cmd.current_dir(cwd);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if shutdown_requested(&ctx.cancel) {
        return Err(AttemptFailure {
            message: shutdown_error(lang),
            fetch_status: first_http_status,
            send_status: last_http_status,
            output: Some(payload.clone()),
            steps_log: steps_log.clone(),
        });
    }

    let mut child = cmd.spawn().map_err(|e| {
        fail_step(
            lang,
            &step_label,
            &format!("запуск: {e}"),
            first_http_status,
            last_http_status,
            payload,
            steps_log,
            step,
            None,
            None,
            None,
        )
    })?;

    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let mut cancel = ctx.cancel.clone();
    let timeout = Duration::from_secs(ctx.http_timeout_secs);

    let wait_fut = async {
        let stdout_task = async {
            let mut buf = Vec::new();
            if let Some(ref mut out) = stdout {
                out.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
            }
            Ok::<_, String>(buf)
        };
        let stderr_task = async {
            let mut buf = Vec::new();
            if let Some(ref mut err) = stderr {
                err.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
            }
            Ok::<_, String>(buf)
        };
        let (stdout_res, stderr_res) = tokio::join!(stdout_task, stderr_task);
        let status = child.wait().await.map_err(|e| e.to_string())?;
        Ok::<_, String>((status, stdout_res?, stderr_res?))
    };

    let (status, stdout_bytes, stderr_bytes) = tokio::select! {
        res = wait_fut => res.map_err(|e| {
            fail_step(
                lang,
                &step_label,
                &e,
                first_http_status,
                last_http_status,
                payload,
                steps_log,
                step,
                None,
                None,
                None,
            )
        })?,
        _ = tokio::time::sleep(timeout) => {
            let _ = child.kill().await;
            return Err(fail_step(
                lang,
                &step_label,
                "таймаут",
                first_http_status,
                last_http_status,
                payload,
                steps_log,
                step,
                None,
                None,
                None,
            ));
        }
        _ = wait_for_shutdown(&mut cancel) => {
            let _ = child.kill().await;
            return Err(AttemptFailure {
                message: shutdown_error(lang),
                fetch_status: first_http_status,
                send_status: last_http_status,
                output: Some(payload.clone()),
                steps_log: steps_log.clone(),
            });
        }
    };

    if stdout_bytes.len() > MAX_STEP_OUTPUT_BYTES || stderr_bytes.len() > MAX_STEP_OUTPUT_BYTES {
        return Err(fail_step(
            lang,
            &step_label,
            &format!("вывод больше лимита {MAX_STEP_OUTPUT_BYTES} байт"),
            first_http_status,
            last_http_status,
            payload,
            steps_log,
            step,
            None,
            status.code(),
            None,
        ));
    }

    let stdout_text = String::from_utf8_lossy(&stdout_bytes).into_owned();
    let stderr_text = String::from_utf8_lossy(&stderr_bytes).into_owned();
    let combined = if stderr_text.is_empty() {
        stdout_text.clone()
    } else if stdout_text.is_empty() {
        stderr_text.clone()
    } else {
        format!("{stdout_text}\n--- stderr ---\n{stderr_text}")
    };

    if !stdout_text.is_empty() {
        info!(job_id = %job.id, step = %step_label, "command stdout:\n{stdout_text}");
    }
    if !stderr_text.is_empty() {
        info!(job_id = %job.id, step = %step_label, "command stderr:\n{stderr_text}");
    }

    let exit_code = status.code();
    if !status.success() {
        let msg = match lang {
            LogLang::Ru => format!(
                "шаг={step_label}; код выхода={}; вывод: {combined}",
                exit_code.map(|c| c.to_string()).unwrap_or_else(|| "—".into())
            ),
            LogLang::En => format!(
                "step={step_label}; exit_code={}; output: {combined}",
                exit_code.map(|c| c.to_string()).unwrap_or_else(|| "—".into())
            ),
        };
        steps_log.push(StepLogEntry {
            id: step.id.clone(),
            kind: StepKind::Command,
            name: step.name.clone(),
            http_status: None,
            exit_code,
            output: Some(combined),
            error: Some(msg.clone()),
        });
        return Err(AttemptFailure {
            message: msg,
            fetch_status: first_http_status,
            send_status: last_http_status,
            output: Some(payload.clone()),
            steps_log: steps_log.clone(),
        });
    }

    debug!(
        job_id = %job.id,
        exit_code = ?exit_code,
        "{}", LogMsg::StepCommandDone.text(lang)
    );

    steps_log.push(StepLogEntry {
        id: step.id.clone(),
        kind: StepKind::Command,
        name: step.name.clone(),
        http_status: None,
        exit_code,
        output: Some(combined.clone()),
        error: None,
    });

    if step.capture_output {
        *payload = stdout_text;
    }
    Ok(())
}

fn fail_step(
    lang: LogLang,
    step_label: &str,
    detail: &str,
    fetch_status: Option<i32>,
    send_status: Option<i32>,
    payload: &str,
    steps_log: &mut Vec<StepLogEntry>,
    step: &JobStep,
    http_status: Option<i32>,
    exit_code: Option<i32>,
    output: Option<String>,
) -> AttemptFailure {
    let message = step_error_simple(lang, step_label, detail);
    steps_log.push(StepLogEntry {
        id: step.id.clone(),
        kind: step.kind.clone(),
        name: step.name.clone(),
        http_status,
        exit_code,
        output,
        error: Some(message.clone()),
    });
    AttemptFailure {
        message,
        fetch_status,
        send_status,
        output: Some(payload.to_string()),
        steps_log: steps_log.clone(),
    }
}

fn flatten_command_args(args: &[String]) -> Vec<String> {
    args.iter()
        .flat_map(|a| a.split_whitespace().map(str::to_string))
        .collect()
}

fn step_label(step: &JobStep) -> String {
    step.name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| match step.kind {
            StepKind::Http => "http".into(),
            StepKind::Transform => "transform".into(),
            StepKind::Command => "command".into(),
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
        "DELETE" => client.delete(url),
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
            if !headers.contains_key(CONTENT_TYPE)
                && matches!(method.as_str(), "POST" | "PUT")
            {
                // Content-Type задаётся ниже через headers clone — добавим в builder
            }
            req = req.body(b.to_string());
        }
    }

    // Для POST/PUT без Content-Type — application/json
    if matches!(method.as_str(), "POST" | "PUT") && !headers.contains_key(CONTENT_TYPE) {
        req = req.header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
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

/// Читает тело ответа как байты и проверяет UTF-8.
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
    if byte_len > MAX_STEP_OUTPUT_BYTES {
        let size_detail = match lang {
            LogLang::Ru => format!("тело больше лимита {MAX_STEP_OUTPUT_BYTES} байт"),
            LogLang::En => format!("body exceeds limit of {MAX_STEP_OUTPUT_BYTES} bytes"),
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
    // ponytail: в тексте ошибки HTTP оставляем короткий фрагмент; полный body — в steps_log
    let preview = truncate(body, 200);
    match lang {
        LogLang::Ru => format!("шаг={step}; url={url}; HTTP={status}; ответ: {preview}"),
        LogLang::En => format!("step={step}; url={url}; HTTP={status}; response: {preview}"),
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
        .unwrap_or_else(|| "—".into());
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

fn truncate(s: &str, max: usize) -> String {
    truncate_preview(s, max).0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::JobStep;

    #[test]
    fn transform_maps_users() {
        let input = r#"{"users":[{"name":"John"}]}"#;
        let script = "return input.users.map(x => ({ username: x.name }));";
        let out = run_transform(script, input).unwrap();
        assert!(out.contains("username"));
    }

    #[test]
    fn resolve_body_from_payload() {
        let mut step = JobStep::new_http();
        step.body_from_payload = true;
        assert_eq!(
            resolve_http_body(&step, r#"{"a":1}"#).as_deref(),
            Some(r#"{"a":1}"#)
        );
    }

    #[test]
    fn flatten_splits_spaced_token() {
        let args = vec!["-t processor".into()];
        assert_eq!(
            flatten_command_args(&args),
            vec!["-t".to_string(), "processor".to_string()]
        );
    }

    #[tokio::test]
    async fn command_echo_stdout() {
        let mut cmd = if cfg!(windows) {
            let mut c = Command::new("cmd");
            c.args(["/C", "echo", "hello-steps"]);
            c
        } else {
            let mut c = Command::new("echo");
            c.arg("hello-steps");
            c
        };
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let out = cmd.output().await.expect("spawn echo");
        assert!(out.status.success());
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("hello-steps"));
    }
}
