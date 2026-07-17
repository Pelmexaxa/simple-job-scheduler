//! Доменные типы: задачи, шаги пайплайна, журнал выполнения и статистика панели.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Состояние задачи в API (вычисляется из флага `enabled`, последнего запуска и факта выполнения).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Draft,
    Active,
    Paused,
    Running,
    Succeeded,
    Failed,
    Disabled,
}

/// Тип расписания запуска задачи.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleType {
    /// Периодический интервал (`5m`, `2h`, `1d`).
    Interval,
    /// Cron-выражение.
    Cron,
    /// Однократный запуск в указанный момент.
    OneTime,
}

/// Тип шага пайплайна.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Http,
    Transform,
    Command,
}

fn default_true() -> bool {
    true
}

/// Один шаг задачи (порядок = порядок в массиве `steps`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobStep {
    pub id: String,
    pub kind: StepKind,
    #[serde(default)]
    pub name: Option<String>,
    // http
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub headers: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub body_from_payload: bool,
    // transform
    #[serde(default)]
    pub script: Option<String>,
    // command
    #[serde(default)]
    pub program: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    /// Писать stdout/тело ответа в общий `payload` для следующих шагов.
    #[serde(default = "default_true")]
    pub capture_output: bool,
}

impl JobStep {
    pub fn new_http() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            kind: StepKind::Http,
            name: Some("HTTP".into()),
            method: Some("GET".into()),
            url: None,
            headers: Some("{}".into()),
            body: None,
            body_from_payload: false,
            script: None,
            program: None,
            args: Vec::new(),
            cwd: None,
            capture_output: true,
        }
    }
}

/// Результат одного шага для журнала выполнения.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepLogEntry {
    pub id: String,
    pub kind: StepKind,
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Строка таблицы `jobs` как возвращается из SQLx.
#[derive(Debug, Clone, FromRow)]
pub struct JobRow {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub job_group: Option<String>,
    pub enabled: i64,
    pub schedule_type: String,
    pub schedule_value: String,
    pub steps: String,
    // legacy (только для миграции со старых БД)
    pub fetch_enabled: i64,
    pub fetch_method: Option<String>,
    pub fetch_url: Option<String>,
    pub fetch_headers: Option<String>,
    pub fetch_body: Option<String>,
    pub transform_enabled: i64,
    pub transform_script: Option<String>,
    pub send_enabled: i64,
    pub send_method: Option<String>,
    pub send_url: Option<String>,
    pub send_headers: Option<String>,
    pub send_body_template: Option<String>,
    pub retry_enabled: i64,
    pub max_retries: Option<i64>,
    pub retry_interval_seconds: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
}

impl JobRow {
    /// Разбирает JSON-массив шагов; при ошибке — пустой список.
    pub fn parsed_steps(&self) -> Vec<JobStep> {
        parse_steps_json(&self.steps)
    }
}

/// Задача в формате REST API с типизированными полями и вычисленным состоянием.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub job_group: Option<String>,
    pub enabled: bool,
    pub state: JobState,
    pub schedule_type: ScheduleType,
    pub schedule_value: String,
    pub steps: Vec<JobStep>,
    pub retry_enabled: bool,
    pub max_retries: Option<i32>,
    pub retry_interval_seconds: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_status: Option<String>,
}

/// Тело запроса на создание или обновление задачи.
#[derive(Debug, Clone, Deserialize)]
pub struct JobInput {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub job_group: Option<String>,
    pub enabled: bool,
    pub schedule_type: ScheduleType,
    pub schedule_value: String,
    #[serde(default)]
    pub steps: Vec<JobStep>,
    pub retry_enabled: bool,
    pub max_retries: Option<i32>,
    pub retry_interval_seconds: Option<i32>,
}

/// Тело запроса на включение/отключение всех задач в группе.
#[derive(Debug, Clone, Deserialize)]
pub struct GroupEnabledInput {
    pub job_group: String,
    pub enabled: bool,
}

/// Ответ на массовое изменение `enabled` по группе.
#[derive(Debug, Clone, Serialize)]
pub struct GroupEnabledResult {
    pub updated: u64,
}

/// Строка таблицы `execution_logs`.
#[derive(Debug, Clone, FromRow)]
pub struct ExecutionLogRow {
    pub id: String,
    pub job_id: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: String,
    pub fetch_status: Option<i64>,
    pub send_status: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error_message: Option<String>,
    pub response_preview: Option<String>,
    pub preview_truncated: i64,
    pub steps_log: Option<String>,
}

/// Запись журнала выполнения для API.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionLog {
    pub id: String,
    pub job_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub fetch_status: Option<i32>,
    pub send_status: Option<i32>,
    pub duration_ms: Option<i64>,
    pub error_message: Option<String>,
    /// Полный финальный payload (без обрезки; потолок = лимит чтения тела).
    pub response_preview: Option<String>,
    pub preview_truncated: bool,
    pub steps_log: Option<Vec<StepLogEntry>>,
}

/// Публичные настройки для веб-интерфейса.
#[derive(Debug, Clone, Serialize)]
pub struct PublicSettings {
    /// Жёсткий потолок размера вывода шага (байты); при превышении — ошибка, не silent truncate.
    pub max_step_output_bytes: u32,
}

/// Агрегированная статистика для страницы «Обзор».
#[derive(Debug, Clone, Serialize)]
pub struct DashboardStats {
    pub active_jobs: i64,
    pub paused_jobs: i64,
    pub failed_jobs: i64,
    pub total_executions: i64,
    pub succeeded_executions: i64,
    pub failed_executions: i64,
    pub recent_logs: Vec<ExecutionLog>,
}

impl JobRow {
    /// Преобразует строку БД в объект API с учётом последнего статуса и флага «выполняется».
    pub fn into_job(self, last_status: Option<String>, running: bool) -> Job {
        let enabled = self.enabled != 0;
        let state = if running {
            JobState::Running
        } else if !enabled {
            JobState::Paused
        } else {
            match last_status.as_deref() {
                Some("failed") => JobState::Failed,
                _ => JobState::Active,
            }
        };

        Job {
            id: self.id,
            name: self.name,
            description: self.description,
            job_group: normalize_job_group(self.job_group),
            enabled,
            state,
            schedule_type: parse_schedule_type(&self.schedule_type),
            schedule_value: self.schedule_value,
            steps: parse_steps_json(&self.steps),
            retry_enabled: self.retry_enabled != 0,
            max_retries: self.max_retries.map(|v| v as i32),
            retry_interval_seconds: self.retry_interval_seconds.map(|v| v as i32),
            created_at: parse_dt(&self.created_at),
            updated_at: parse_dt(&self.updated_at),
            last_run_at: self.last_run_at.as_deref().map(parse_dt),
            next_run_at: self.next_run_at.as_deref().map(parse_dt),
            last_status,
        }
    }
}

impl ExecutionLogRow {
    /// Преобразует строку БД в объект API с разобранными датами.
    pub fn into_log(self) -> ExecutionLog {
        let steps_log = self
            .steps_log
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        ExecutionLog {
            id: self.id,
            job_id: self.job_id,
            started_at: parse_dt(&self.started_at),
            finished_at: self.finished_at.as_deref().map(parse_dt),
            status: self.status,
            fetch_status: self.fetch_status.map(|v| v as i32),
            send_status: self.send_status.map(|v| v as i32),
            duration_ms: self.duration_ms,
            error_message: self.error_message,
            response_preview: self.response_preview,
            preview_truncated: self.preview_truncated != 0,
            steps_log,
        }
    }
}

/// Сериализует шаги в JSON для колонки `jobs.steps`.
pub fn steps_to_json(steps: &[JobStep]) -> String {
    serde_json::to_string(steps).unwrap_or_else(|_| "[]".to_string())
}

/// Разбирает JSON шагов; битый JSON → пустой список.
pub fn parse_steps_json(raw: &str) -> Vec<JobStep> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    serde_json::from_str(trimmed).unwrap_or_default()
}

/// Собирает шаги из legacy-колонок fetch/transform/send.
pub fn legacy_columns_to_steps(
    fetch_enabled: bool,
    fetch_method: Option<&str>,
    fetch_url: Option<&str>,
    fetch_headers: Option<&str>,
    fetch_body: Option<&str>,
    transform_enabled: bool,
    transform_script: Option<&str>,
    send_enabled: bool,
    send_method: Option<&str>,
    send_url: Option<&str>,
    send_headers: Option<&str>,
    send_body_template: Option<&str>,
) -> Vec<JobStep> {
    let mut steps = Vec::new();
    if fetch_enabled {
        steps.push(JobStep {
            id: Uuid::new_v4().to_string(),
            kind: StepKind::Http,
            name: Some("Fetch".into()),
            method: Some(fetch_method.unwrap_or("GET").to_string()),
            url: fetch_url.map(|s| s.to_string()),
            headers: fetch_headers.map(|s| s.to_string()),
            body: fetch_body.map(|s| s.to_string()),
            body_from_payload: false,
            script: None,
            program: None,
            args: Vec::new(),
            cwd: None,
            capture_output: true,
        });
    }
    if transform_enabled {
        steps.push(JobStep {
            id: Uuid::new_v4().to_string(),
            kind: StepKind::Transform,
            name: Some("Transform".into()),
            method: None,
            url: None,
            headers: None,
            body: None,
            body_from_payload: false,
            script: transform_script.map(|s| s.to_string()),
            program: None,
            args: Vec::new(),
            cwd: None,
            capture_output: true,
        });
    }
    if send_enabled {
        let template = send_body_template.unwrap_or("{{payload}}");
        let body_from_payload = template.contains("{{payload}}") || template.trim().is_empty();
        let body = if body_from_payload && template.trim() == "{{payload}}" {
            None
        } else if template.contains("{{payload}}") {
            Some(template.to_string())
        } else {
            Some(template.to_string())
        };
        let use_payload = body_from_payload || body.is_none();
        steps.push(JobStep {
            id: Uuid::new_v4().to_string(),
            kind: StepKind::Http,
            name: Some("Send".into()),
            method: Some(send_method.unwrap_or("POST").to_string()),
            url: send_url.map(|s| s.to_string()),
            headers: send_headers.map(|s| s.to_string()),
            body,
            body_from_payload: use_payload,
            script: None,
            program: None,
            args: Vec::new(),
            cwd: None,
            capture_output: true,
        });
    }
    steps
}

/// Разбирает строковое значение типа расписания из БД.
fn parse_schedule_type(s: &str) -> ScheduleType {
    match s {
        "cron" => ScheduleType::Cron,
        "one_time" => ScheduleType::OneTime,
        _ => ScheduleType::Interval,
    }
}

/// Кодирует тип расписания для записи в БД.
pub fn schedule_type_str(t: &ScheduleType) -> &'static str {
    match t {
        ScheduleType::Interval => "interval",
        ScheduleType::Cron => "cron",
        ScheduleType::OneTime => "one_time",
    }
}

/// Парсит RFC3339; при ошибке возвращает текущее время UTC.
fn parse_dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

/// Текущий момент в формате RFC3339 для полей БД.
pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

/// Нормализует название группы: обрезка пробелов; пустая строка → `None`.
pub fn normalize_job_group(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Максимальная длина названия группы задач.
pub const JOB_GROUP_MAX_LEN: usize = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_roundtrip_json() {
        let mut step = JobStep::new_http();
        step.url = Some("https://example.com".into());
        let json = steps_to_json(&[step.clone()]);
        let parsed = parse_steps_json(&json);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].kind, StepKind::Http);
        assert_eq!(parsed[0].url.as_deref(), Some("https://example.com"));
    }

    #[test]
    fn legacy_three_steps() {
        let steps = legacy_columns_to_steps(
            true,
            Some("GET"),
            Some("https://a"),
            None,
            None,
            true,
            Some("return input;"),
            true,
            Some("POST"),
            Some("https://b"),
            None,
            Some("{{payload}}"),
        );
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].kind, StepKind::Http);
        assert_eq!(steps[1].kind, StepKind::Transform);
        assert_eq!(steps[2].kind, StepKind::Http);
        assert!(steps[2].body_from_payload);
    }
}
