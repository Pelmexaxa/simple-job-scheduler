//! Доменные типы: задачи, расписание, журнал выполнения и статистика панели.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

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
    pub fetch_enabled: bool,
    pub fetch_method: Option<String>,
    pub fetch_url: Option<String>,
    pub fetch_headers: Option<String>,
    pub fetch_body: Option<String>,
    pub transform_enabled: bool,
    pub transform_script: Option<String>,
    pub send_enabled: bool,
    pub send_method: Option<String>,
    pub send_url: Option<String>,
    pub send_headers: Option<String>,
    pub send_body_template: Option<String>,
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
    pub fetch_enabled: bool,
    pub fetch_method: Option<String>,
    pub fetch_url: Option<String>,
    pub fetch_headers: Option<String>,
    pub fetch_body: Option<String>,
    pub transform_enabled: bool,
    pub transform_script: Option<String>,
    pub send_enabled: bool,
    pub send_method: Option<String>,
    pub send_url: Option<String>,
    pub send_headers: Option<String>,
    pub send_body_template: Option<String>,
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
    pub response_preview: Option<String>,
    pub preview_truncated: bool,
}

/// Публичные настройки для веб-интерфейса.
#[derive(Debug, Clone, Serialize)]
pub struct PublicSettings {
    pub log_response_preview_max_bytes: u32,
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
            fetch_enabled: self.fetch_enabled != 0,
            fetch_method: self.fetch_method,
            fetch_url: self.fetch_url,
            fetch_headers: self.fetch_headers,
            fetch_body: self.fetch_body,
            transform_enabled: self.transform_enabled != 0,
            transform_script: self.transform_script,
            send_enabled: self.send_enabled != 0,
            send_method: self.send_method,
            send_url: self.send_url,
            send_headers: self.send_headers,
            send_body_template: self.send_body_template,
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
        }
    }
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
