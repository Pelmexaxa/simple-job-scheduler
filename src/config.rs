//! Конфигурация приложения из переменных окружения с префиксом `AJS_`.

use std::env;

/// Параметры запуска планировщика, загружаемые при старте процесса.
#[derive(Clone, Debug)]
pub struct AppConfig {
    /// Адрес привязки HTTP-сервера (например, `0.0.0.0`).
    pub host: String,
    /// Порт HTTP-сервера.
    pub port: u16,
    /// Путь к файлу базы данных SQLite.
    pub db_path: String,
    /// Уровень логирования (`info`, `debug` и т.д.).
    pub log_level: String,
    /// Язык интерфейса и серверных логов по умолчанию (`en` или `ru`).
    pub default_language: String,
    /// Максимальное число одновременно выполняемых задач.
    pub max_concurrent_jobs: usize,
    /// Таймаут HTTP-запросов при выполнении задач (секунды).
    pub http_timeout_seconds: u64,
    /// Интервал опроса планировщика (миллисекунды).
    pub job_tick_interval_ms: u64,
    /// Разрешить JavaScript-преобразование ответов.
    pub enable_js_transform: bool,
    /// Срок хранения записей журнала выполнения (дни).
    pub retention_days: u32,
    /// Каталог файлов серверных логов; по умолчанию `./logs` относительно каталога запуска.
    pub log_dir: String,
    /// При старте выполнять задачи с просроченным `next_run_at` (иначе — пересчитать расписание от «сейчас»).
    pub run_overdue_on_startup: bool,
    /// При старте отключить все задачи (`enabled = 0`); включение — вручную в UI.
    pub disable_all_jobs_on_startup: bool,
}

impl AppConfig {
    /// Читает конфигурацию из переменных окружения; для отсутствующих ключей используются значения по умолчанию.
    pub fn from_env() -> Self {
        Self {
            host: env_string("AJS_HOST", "127.0.0.1"),
            port: env_parse("AJS_PORT", 6378),
            db_path: env_string("AJS_DB_PATH", "./scheduler.db"),
            log_level: env_string("AJS_LOG_LEVEL", "info"),
            default_language: env_string("AJS_DEFAULT_LANGUAGE", "en"),
            max_concurrent_jobs: env_parse("AJS_MAX_CONCURRENT_JOBS", 10),
            http_timeout_seconds: env_parse("AJS_HTTP_TIMEOUT_SECONDS", 60),
            job_tick_interval_ms: env_parse("AJS_JOB_TICK_INTERVAL_MS", 1000),
            enable_js_transform: env_bool("AJS_ENABLE_JS_TRANSFORM", true),
            retention_days: env_parse("AJS_RETENTION_DAYS", 30),
            log_dir: env_string("AJS_LOG_DIR", "./logs"),
            run_overdue_on_startup: env_bool("AJS_RUN_OVERDUE_ON_STARTUP", true),
            disable_all_jobs_on_startup: env_bool("AJS_DISABLE_ALL_JOBS_ON_STARTUP", false),
        }
    }

    /// Возвращает строку вида `host:port` для привязки сокета.
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Язык сообщений в серверном журнале (`tracing`).
    pub fn log_lang(&self) -> crate::i18n::LogLang {
        crate::i18n::LogLang::from_code(&self.default_language)
    }
}

/// Читает строковую переменную окружения или возвращает значение по умолчанию.
fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Читает переменную окружения и парсит её в число; при ошибке — значение по умолчанию.
fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Читает логическую переменную окружения (`1`, `true`, `yes` — истина).
fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}
