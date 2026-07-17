//! Локализация серверных логов (ru / en) по параметру `AJS_DEFAULT_LANGUAGE`.

/// Язык сообщений в журнале сервера.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLang {
    Ru,
    En,
}

impl LogLang {
    /// Разбирает код языка (`ru`, `en`); неизвестные значения → английский.
    pub fn from_code(code: &str) -> Self {
        match code.to_lowercase().as_str() {
            "ru" => LogLang::Ru,
            _ => LogLang::En,
        }
    }
}

/// Ключи локализованных сообщений для `tracing`.
#[derive(Clone, Copy, Debug)]
pub enum LogMsg {
    ServerStarting,
    ConfigLoaded,
    ConfigSummary,
    DbConnecting,
    DbConnected,
    MigrationsApplied,
    SchedulesInitialized,
    SchedulesInitCount,
    SchedulerStarted,
    ServerListening,
    SchedulerTick,
    SchedulerDueJobs,
    SchedulerTickError,
    LogPurgeError,
    LogPurgeDone,
    ManualRunFailed,
    ScheduledRunFailed,
    JobRunStart,
    JobRunSuccess,
    JobRunFailed,
    JobAlreadyRunning,
    JobScheduleClaimSkipped,
    JobNotFound,
    JobExecutionStart,
    JobExecutionSuccess,
    JobExecutionFailed,
    JobRetryAttempt,
    PipelineStepFailed,
    StepHttp,
    StepHttpDone,
    StepTransform,
    StepTransformDone,
    StepTransformSkipped,
    StepCommand,
    StepCommandDone,
    HttpOutbound,
    ApiRequest,
    ApiResponse,
    ApiError,
    JobCreated,
    JobUpdated,
    JobDeleted,
    JobRunQueued,
    ShutdownRequested,
    ShutdownSignalError,
    HttpServerStopped,
    SchedulerStopped,
    ShutdownDrainTimeout,
    ShutdownComplete,
    ShutdownInProgress,
    ShutdownWaitingJobs,
    ShutdownJobsAborted,
    ShutdownPoolCloseTimeout,
    JobCancelled,
    StartupAllJobsDisabled,
    StartupOverdueRescheduled,
}

impl LogMsg {
    /// Возвращает текст сообщения на выбранном языке.
    pub fn text(self, lang: LogLang) -> &'static str {
        use LogLang::{En, Ru};
        use LogMsg::*;

        match (lang, self) {
            // --- запуск и конфигурация ---
            (Ru, ServerStarting) => "Запуск simple-job-scheduler",
            (En, ServerStarting) => "Starting simple-job-scheduler",

            (Ru, ConfigLoaded) => "Конфигурация загружена из переменных окружения",
            (En, ConfigLoaded) => "Configuration loaded from environment variables",

            (Ru, ConfigSummary) => {
                "Параметры: host={host}, port={port}, db={db}, log_level={level}, lang={lang}, \
                 log_dir={log_dir}, max_jobs={max_jobs}, http_timeout={timeout}s, tick={tick}ms, js={js}, retention={retention}d, \
                 run_overdue_on_startup={run_overdue}, disable_all_jobs_on_startup={disable_all}"
            }
            (En, ConfigSummary) => {
                "Settings: host={host}, port={port}, db={db}, log_level={level}, lang={lang}, \
                 log_dir={log_dir}, max_jobs={max_jobs}, http_timeout={timeout}s, tick={tick}ms, js={js}, retention={retention}d, \
                 run_overdue_on_startup={run_overdue}, disable_all_jobs_on_startup={disable_all}"
            }

            // --- база данных ---
            (Ru, DbConnecting) => "Подключение к SQLite",
            (En, DbConnecting) => "Connecting to SQLite",

            (Ru, DbConnected) => "Подключение к базе данных установлено",
            (En, DbConnected) => "Database connection established",

            (Ru, MigrationsApplied) => "Миграции схемы применены",
            (En, MigrationsApplied) => "Schema migrations applied",

            // --- планировщик ---
            (Ru, SchedulesInitialized) => "Расписания задач инициализированы",
            (En, SchedulesInitialized) => "Job schedules initialized",

            (Ru, StartupAllJobsDisabled) => "При старте все задачи отключены",
            (En, StartupAllJobsDisabled) => "All jobs disabled on startup",

            (Ru, StartupOverdueRescheduled) => {
                "Просроченные задачи не запускались: next_run_at пересчитан от текущего времени"
            }
            (En, StartupOverdueRescheduled) => {
                "Overdue jobs were not run: next_run_at recalculated from now"
            }

            (Ru, SchedulesInitCount) => "Обновлено расписаний для задач без next_run_at",
            (En, SchedulesInitCount) => "Updated schedules for jobs missing next_run_at",

            (Ru, SchedulerStarted) => "Фоновый цикл планировщика запущен",
            (En, SchedulerStarted) => "Scheduler tick loop started",

            (Ru, ServerListening) => "HTTP-сервер слушает входящие соединения",
            (En, ServerListening) => "HTTP server listening for connections",

            (Ru, ShutdownRequested) => "Получен сигнал остановки (Ctrl+C), завершение работы…",
            (En, ShutdownRequested) => "Shutdown signal received (Ctrl+C), stopping…",

            (Ru, ShutdownSignalError) => "Не удалось установить обработчик Ctrl+C",
            (En, ShutdownSignalError) => "Failed to install Ctrl+C handler",

            (Ru, HttpServerStopped) => "HTTP-сервер остановлен, активные запросы обработаны",
            (En, HttpServerStopped) => "HTTP server stopped; in-flight requests finished",

            (Ru, SchedulerStopped) => "Планировщик остановлен",
            (En, SchedulerStopped) => "Scheduler stopped",

            (Ru, ShutdownDrainTimeout) => {
                "Таймаут ожидания задач при остановке; часть выполнений ещё активна"
            }
            (En, ShutdownDrainTimeout) => {
                "Shutdown drain timed out; some job runs are still active"
            }

            (Ru, ShutdownComplete) => "Программа завершена корректно",
            (En, ShutdownComplete) => "Shutdown complete",

            (Ru, ShutdownInProgress) => "Сервер завершает работу, новые запуски недоступны",
            (En, ShutdownInProgress) => "Server is shutting down; new runs are not accepted",

            (Ru, ShutdownWaitingJobs) => "Ожидание завершения активных задач",
            (En, ShutdownWaitingJobs) => "Waiting for active job runs to finish",

            (Ru, ShutdownJobsAborted) => "Фоновые задачи прерваны при остановке",
            (En, ShutdownJobsAborted) => "Background job tasks aborted on shutdown",

            (Ru, ShutdownPoolCloseTimeout) => {
                "Таймаут закрытия пула БД; завершение процесса без ожидания"
            }
            (En, ShutdownPoolCloseTimeout) => {
                "Database pool close timed out; exiting without waiting further"
            }

            (Ru, JobCancelled) => "Выполнение прервано: сервер останавливается",
            (En, JobCancelled) => "Run cancelled: server is shutting down",

            (Ru, SchedulerTick) => "Тик планировщика",
            (En, SchedulerTick) => "Scheduler tick",

            (Ru, SchedulerDueJobs) => "Найдены задачи к запуску по расписанию",
            (En, SchedulerDueJobs) => "Due jobs found for scheduled execution",

            (Ru, SchedulerTickError) => "Ошибка тика планировщика",
            (En, SchedulerTickError) => "Scheduler tick error",

            (Ru, LogPurgeError) => "Ошибка очистки журнала выполнения",
            (En, LogPurgeError) => "Execution log purge error",

            (Ru, LogPurgeDone) => "Удалены устаревшие записи журнала выполнения",
            (En, LogPurgeDone) => "Purged old execution log entries",

            (Ru, ManualRunFailed) => "Ручной запуск задачи завершился с ошибкой",
            (En, ManualRunFailed) => "Manual job run failed",

            (Ru, ScheduledRunFailed) => "Плановый запуск задачи завершился с ошибкой",
            (En, ScheduledRunFailed) => "Scheduled job run failed",

            (Ru, JobRunStart) => "Начало выполнения задачи",
            (En, JobRunStart) => "Job execution started",

            (Ru, JobRunSuccess) => "Задача выполнена успешно",
            (En, JobRunSuccess) => "Job completed successfully",

            (Ru, JobRunFailed) => "Выполнение задачи завершилось с ошибкой",
            (En, JobRunFailed) => "Job execution failed",

            (Ru, JobAlreadyRunning) => "Задача уже выполняется",
            (En, JobAlreadyRunning) => "Job is already running",

            (Ru, JobScheduleClaimSkipped) => "Плановый запуск пропущен: next_run_at уже обновлён",
            (En, JobScheduleClaimSkipped) => "Scheduled run skipped: next_run_at already claimed",

            (Ru, JobNotFound) => "Задача не найдена",
            (En, JobNotFound) => "Job not found",

            // --- пайплайн выполнения ---
            (Ru, JobExecutionStart) => "Старт пайплайна задачи",
            (En, JobExecutionStart) => "Job pipeline started",

            (Ru, JobExecutionSuccess) => "Пайплайн задачи завершён успешно",
            (En, JobExecutionSuccess) => "Job pipeline finished successfully",

            (Ru, JobExecutionFailed) => "Пайплайн задачи завершён с ошибкой",
            (En, JobExecutionFailed) => "Job pipeline finished with error",

            (Ru, JobRetryAttempt) => "Повторная попытка выполнения задачи",
            (En, JobRetryAttempt) => "Retrying job execution",

            (Ru, PipelineStepFailed) => "Ошибка шага пайплайна задачи",
            (En, PipelineStepFailed) => "Job pipeline step failed",

            (Ru, StepHttp) => "Шаг http: исходящий HTTP-запрос",
            (En, StepHttp) => "HTTP step: outbound request",

            (Ru, StepHttpDone) => "Шаг http завершён",
            (En, StepHttpDone) => "HTTP step completed",

            (Ru, StepTransform) => "Шаг transform: JS-преобразование",
            (En, StepTransform) => "Transform step: JS transformation",

            (Ru, StepTransformDone) => "Шаг transform завершён",
            (En, StepTransformDone) => "Transform step completed",

            (Ru, StepTransformSkipped) => "Шаг transform пропущен (JS отключён)",
            (En, StepTransformSkipped) => "Transform step skipped (JS disabled)",

            (Ru, StepCommand) => "Шаг command: локальная команда",
            (En, StepCommand) => "Command step: local process",

            (Ru, StepCommandDone) => "Шаг command завершён",
            (En, StepCommandDone) => "Command step completed",

            (Ru, HttpOutbound) => "Исходящий HTTP-запрос",
            (En, HttpOutbound) => "Outbound HTTP request",

            // --- API ---
            (Ru, ApiRequest) => "Входящий HTTP-запрос",
            (En, ApiRequest) => "Incoming HTTP request",

            (Ru, ApiResponse) => "HTTP-ответ отправлен",
            (En, ApiResponse) => "HTTP response sent",

            (Ru, ApiError) => "Ошибка обработки HTTP-запроса",
            (En, ApiError) => "HTTP request handler error",

            (Ru, JobCreated) => "Задача создана через API",
            (En, JobCreated) => "Job created via API",

            (Ru, JobUpdated) => "Задача обновлена через API",
            (En, JobUpdated) => "Job updated via API",

            (Ru, JobDeleted) => "Задача удалена через API",
            (En, JobDeleted) => "Job deleted via API",

            (Ru, JobRunQueued) => "Задача поставлена в очередь на ручной запуск",
            (En, JobRunQueued) => "Job queued for manual run",
        }
    }
}

/// Ключи сообщений валидации API (ru / en).
#[derive(Clone, Copy, Debug)]
pub enum ValMsg {
    NameRequired,
    JobGroupInvalid,
    ScheduleValueRequired,
    ScheduleIntervalInvalid,
    ScheduleCronInvalid,
    ScheduleOneTimeInvalid,
    ScheduleOneTimePast,
    FetchUrlRequired,
    FetchUrlInvalid,
    FetchMethodInvalid,
    FetchHeadersInvalid,
    TransformScriptRequired,
    SendUrlRequired,
    SendUrlInvalid,
    SendMethodInvalid,
    SendHeadersInvalid,
    HttpUrlRequired,
    HttpUrlInvalid,
    HttpMethodInvalid,
    HttpHeadersInvalid,
    CommandProgramRequired,
    CommandArgInvalid,
    MaxRetriesInvalid,
    RetryIntervalInvalid,
}

impl ValMsg {
    /// Текст ошибки валидации на выбранном языке.
    pub fn text(self, lang: LogLang) -> &'static str {
        use LogLang::{En, Ru};
        use ValMsg::*;

        match (lang, self) {
            (Ru, NameRequired) => "Укажите название задачи",
            (En, NameRequired) => "Job name is required",

            (Ru, JobGroupInvalid) => {
                "Группа: до 64 символов, без управляющих символов"
            }
            (En, JobGroupInvalid) => {
                "Group: up to 64 characters, no control characters"
            }

            (Ru, ScheduleValueRequired) => "Укажите значение расписания",
            (En, ScheduleValueRequired) => "Schedule value is required",

            (Ru, ScheduleIntervalInvalid) => {
                "Некорректный интервал: используйте формат 5m, 2h, 1d (значение больше 0)"
            }
            (En, ScheduleIntervalInvalid) => {
                "Invalid interval: use 5m, 2h, 1d format (value must be greater than 0)"
            }

            (Ru, ScheduleCronInvalid) => "Некорректное cron-выражение",
            (En, ScheduleCronInvalid) => "Invalid cron expression",

            (Ru, ScheduleOneTimeInvalid) => {
                "Некорректная дата: RFC3339 или ГГГГ-ММ-ДД ЧЧ:ММ:СС (UTC)"
            }
            (En, ScheduleOneTimeInvalid) => {
                "Invalid date: use RFC3339 or YYYY-MM-DD HH:MM:SS (UTC)"
            }

            (Ru, ScheduleOneTimePast) => "Время однократного запуска должно быть в будущем",
            (En, ScheduleOneTimePast) => "One-time run must be scheduled in the future",

            (Ru, FetchUrlRequired) => "Укажите URL для шага получения",
            (En, FetchUrlRequired) => "Fetch URL is required when fetch is enabled",

            (Ru, FetchUrlInvalid) => "URL получения должен начинаться с http:// или https://",
            (En, FetchUrlInvalid) => "Fetch URL must start with http:// or https://",

            (Ru, FetchMethodInvalid) => "Метод получения: GET или POST",
            (En, FetchMethodInvalid) => "Fetch method must be GET or POST",

            (Ru, FetchHeadersInvalid) => "Заголовки получения должны быть JSON-объектом",
            (En, FetchHeadersInvalid) => "Fetch headers must be a JSON object",

            (Ru, TransformScriptRequired) => "Укажите JavaScript-скрипт преобразования",
            (En, TransformScriptRequired) => "Transform script is required when transform is enabled",

            (Ru, SendUrlRequired) => "Укажите URL для шага отправки",
            (En, SendUrlRequired) => "Send URL is required when send is enabled",

            (Ru, SendUrlInvalid) => "URL отправки должен начинаться с http:// или https://",
            (En, SendUrlInvalid) => "Send URL must start with http:// or https://",

            (Ru, SendMethodInvalid) => "Метод отправки: POST или PUT",
            (En, SendMethodInvalid) => "Send method must be POST or PUT",

            (Ru, SendHeadersInvalid) => "Заголовки отправки должны быть JSON-объектом",
            (En, SendHeadersInvalid) => "Send headers must be a JSON object",

            (Ru, HttpUrlRequired) => "Укажите URL для HTTP-шага",
            (En, HttpUrlRequired) => "HTTP step URL is required",

            (Ru, HttpUrlInvalid) => "URL HTTP-шага должен начинаться с http:// или https://",
            (En, HttpUrlInvalid) => "HTTP step URL must start with http:// or https://",

            (Ru, HttpMethodInvalid) => "Метод HTTP-шага: GET, POST, PUT или DELETE",
            (En, HttpMethodInvalid) => "HTTP step method must be GET, POST, PUT, or DELETE",

            (Ru, HttpHeadersInvalid) => "Заголовки HTTP-шага должны быть JSON-объектом",
            (En, HttpHeadersInvalid) => "HTTP step headers must be a JSON object",

            (Ru, CommandProgramRequired) => "Укажите программу для шага command",
            (En, CommandProgramRequired) => "Command step program is required",

            (Ru, CommandArgInvalid) => "Аргументы команды не должны содержать NUL",
            (En, CommandArgInvalid) => "Command arguments must not contain NUL bytes",

            (Ru, MaxRetriesInvalid) => "Макс. повторов: целое число ≥ 0",
            (En, MaxRetriesInvalid) => "Max retries must be an integer ≥ 0",

            (Ru, RetryIntervalInvalid) => "Интервал повтора: целое число ≥ 1 сек",
            (En, RetryIntervalInvalid) => "Retry interval must be an integer ≥ 1 second",
        }
    }
}

/// Форматирует сводку конфигурации для записи в лог.
pub fn format_config_summary(
    lang: LogLang,
    host: &str,
    port: u16,
    db: &str,
    level: &str,
    lang_code: &str,
    log_dir: &str,
    max_jobs: usize,
    timeout: u64,
    tick: u64,
    js: bool,
    retention: u32,
    run_overdue_on_startup: bool,
    disable_all_jobs_on_startup: bool,
) -> String {
    let template = LogMsg::ConfigSummary.text(lang);
    template
        .replace("{host}", host)
        .replace("{port}", &port.to_string())
        .replace("{db}", db)
        .replace("{level}", level)
        .replace("{lang}", lang_code)
        .replace("{log_dir}", log_dir)
        .replace("{max_jobs}", &max_jobs.to_string())
        .replace("{timeout}", &timeout.to_string())
        .replace("{tick}", &tick.to_string())
        .replace("{js}", &js.to_string())
        .replace("{retention}", &retention.to_string())
        .replace("{run_overdue}", &run_overdue_on_startup.to_string())
        .replace("{disable_all}", &disable_all_jobs_on_startup.to_string())
}
