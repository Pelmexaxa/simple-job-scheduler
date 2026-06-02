//! Планировщик: тик-цикл, выбор просроченных задач и вычисление следующего запуска.

use crate::config::AppConfig;
use crate::database::{purge_old_logs, DbPool};
use crate::execution::{run_job, ExecutionContext};
use crate::i18n::{LogLang, LogMsg};
use crate::jobs;
use crate::models::{now_rfc3339, ScheduleType};
use chrono::{DateTime, Duration, Utc};
use cron::Schedule;
use std::collections::HashSet;
use std::future::Future;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Semaphore, broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Ожидание снятия задач с учётом `running` после отмены (секунды).
const SHUTDOWN_DRAIN_TIMEOUT: StdDuration = StdDuration::from_secs(5);

/// Снимает `job_id` из `running` при завершении или отмене фоновой задачи.
struct RunningGuard {
    running: Arc<StdMutex<HashSet<String>>>,
    job_id: String,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.running.lock() {
            set.remove(&self.job_id);
        }
    }
}

/// Дескриптор фонового планировщика и ручного запуска задач.
#[derive(Clone)]
pub struct SchedulerHandle {
    pool: DbPool,
    config: AppConfig,
    running: Arc<StdMutex<HashSet<String>>>,
    semaphore: Arc<Semaphore>,
    shutdown_tx: broadcast::Sender<()>,
    shutting_down: Arc<AtomicBool>,
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
    job_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
}

impl SchedulerHandle {
    /// Создаёт планировщик с ограничением параллельных выполнений из конфигурации.
    pub fn new(pool: DbPool, config: AppConfig) -> Self {
        let permits = config.max_concurrent_jobs;
        let (shutdown_tx, _) = broadcast::channel(1);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Self {
            pool,
            config,
            running: Arc::new(StdMutex::new(HashSet::new())),
            semaphore: Arc::new(Semaphore::new(permits)),
            shutdown_tx,
            shutting_down: Arc::new(AtomicBool::new(false)),
            cancel_tx,
            cancel_rx,
            job_tasks: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// Останавливает тик-цикл, отменяет фоновые задачи и ждёт их завершения.
    pub async fn shutdown_and_drain(&self) {
        let lang = self.log_lang();
        self.shutting_down.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(());
        let _ = self.cancel_tx.send(true);

        let aborted = {
            let handles: Vec<JoinHandle<()>> = self.job_tasks.lock().unwrap().drain(..).collect();
            let n = handles.len();
            for h in handles {
                h.abort();
            }
            n
        };
        if aborted > 0 {
            info!(
                aborted = aborted,
                "{}",
                LogMsg::ShutdownJobsAborted.text(lang)
            );
        }

        let deadline = Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
        loop {
            let remaining = self.running.lock().unwrap().len();
            if remaining == 0 {
                break;
            }
            info!(
                remaining = remaining,
                "{}",
                LogMsg::ShutdownWaitingJobs.text(lang)
            );
            if Instant::now() >= deadline {
                warn!(
                    remaining = remaining,
                    timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                    "{}",
                    LogMsg::ShutdownDrainTimeout.text(lang)
                );
                self.running.lock().unwrap().clear();
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(100)).await;
        }

        info!("{}", LogMsg::SchedulerStopped.text(lang));
    }

    fn spawn_job_task<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(fut);
        self.job_tasks.lock().unwrap().push(handle);
    }

    fn reject_if_shutting_down(&self) -> Result<(), String> {
        if self.shutting_down.load(Ordering::SeqCst) {
            Err(LogMsg::ShutdownInProgress.text(self.log_lang()).to_string())
        } else {
            Ok(())
        }
    }

    fn log_lang(&self) -> LogLang {
        self.config.log_lang()
    }

    /// Возвращает идентификаторы задач, которые сейчас выполняются.
    pub async fn running_ids(&self) -> Vec<String> {
        self.running.lock().unwrap().iter().cloned().collect()
    }

    /// Запускает задачу в фоне по запросу пользователя (кнопка «Запустить сейчас»).
    pub async fn spawn_manual(&self, job_id: String) {
        if self.reject_if_shutting_down().is_err() {
            warn!(
                job_id = %job_id,
                "{}",
                LogMsg::ShutdownInProgress.text(self.log_lang())
            );
            return;
        }
        let this = self.clone();
        self.spawn_job_task(async move {
            if let Err(e) = this.execute_job_by_id(&job_id, "manual").await {
                error!(
                    job_id = %job_id,
                    error = %e,
                    "{}", LogMsg::ManualRunFailed.text(this.log_lang())
                );
            }
        });
    }

    /// Загружает задачу из БД и выполняет её пайплайн.
    pub async fn execute_job_by_id(&self, job_id: &str, trigger: &str) -> Result<(), String> {
        self.reject_if_shutting_down()?;
        let lang = self.log_lang();
        let row = sqlx::query_as::<_, crate::models::JobRow>(
            "SELECT * FROM jobs WHERE id = ?",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| LogMsg::JobNotFound.text(lang).to_string())?;

        self.run_row(row, trigger).await
    }

    /// Выполняет задачу с блокировкой повторного запуска и учётом семафора.
    async fn run_row(&self, row: crate::models::JobRow, trigger: &str) -> Result<(), String> {
        let lang = self.log_lang();
        let job_id = row.id.clone();
        let job_name = row.name.clone();

        {
            let mut set = self.running.lock().unwrap();
            if set.contains(&job_id) {
                warn!(
                    job_id = %job_id,
                    name = %job_name,
                    "{}", LogMsg::JobAlreadyRunning.text(lang)
                );
                return Err(LogMsg::JobAlreadyRunning.text(lang).to_string());
            }
            set.insert(job_id.clone());
        }
        let _running_guard = RunningGuard {
            running: self.running.clone(),
            job_id: job_id.clone(),
        };

        info!(
            job_id = %job_id,
            name = %job_name,
            trigger = trigger,
            "{}", LogMsg::JobRunStart.text(lang)
        );

        if trigger == "scheduled" {
            let st = schedule_type_from_str(&row.schedule_type);
            if let Some(next) = compute_next_run(&st, &row.schedule_value, None) {
                let now = now_rfc3339();
                if !jobs::claim_next_run(&self.pool, &job_id, &now, &next)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    debug!(
                        job_id = %job_id,
                        name = %job_name,
                        "{}", LogMsg::JobScheduleClaimSkipped.text(lang)
                    );
                    return Ok(());
                }
            }
        }

        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())?;

        let ctx = ExecutionContext {
            pool: self.pool.clone(),
            http_timeout_secs: self.config.http_timeout_seconds,
            enable_js: self.config.enable_js_transform,
            log_lang: lang,
            preview_max_bytes: self.config.log_response_preview_max_bytes,
            cancel: self.cancel_rx.clone(),
        };

        let result = run_job(&ctx, &row).await;

        match &result {
            Ok(()) => info!(
                job_id = %job_id,
                name = %job_name,
                trigger = trigger,
                "{}", LogMsg::JobRunSuccess.text(lang)
            ),
            Err(e) => error!(
                job_id = %job_id,
                name = %job_name,
                trigger = trigger,
                error = %e,
                "{}", LogMsg::JobRunFailed.text(lang)
            ),
        }

        result
    }

    /// Запускает бесконечный цикл опроса просроченных задач и очистки старых логов.
    pub fn start_tick_loop(&self) {
        let interval_ms = self.config.job_tick_interval_ms;
        let retention = self.config.retention_days;
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let this = self.clone();

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    _ = tick.tick() => {
                        if this.shutting_down.load(Ordering::SeqCst) {
                            break;
                        }
                        if let Err(e) = this.tick_once().await {
                            error!(
                                error = %e,
                                "{}", LogMsg::SchedulerTickError.text(this.log_lang())
                            );
                        }
                        if let Err(e) = purge_old_logs(&this.pool, retention, this.log_lang()).await {
                            error!(
                                error = %e,
                                "{}", LogMsg::LogPurgeError.text(this.log_lang())
                            );
                        }
                    }
                }
            }
        });
    }

    /// Один проход: находит задачи с `next_run_at <= now` и запускает их асинхронно.
    async fn tick_once(&self) -> Result<(), sqlx::Error> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Ok(());
        }
        let lang = self.log_lang();
        let now = now_rfc3339();
        let due = jobs::due_jobs(&self.pool, &now).await?;

        let running = self.running.lock().unwrap();
        let to_run: Vec<_> = due
            .into_iter()
            .filter(|row| !running.contains(&row.id))
            .collect();
        drop(running);

        debug!(
            due_count = to_run.len(),
            "{}", LogMsg::SchedulerTick.text(lang)
        );

        if !to_run.is_empty() {
            info!(
                due_count = to_run.len(),
                "{}", LogMsg::SchedulerDueJobs.text(lang)
            );
        }

        for row in to_run {
            let this = self.clone();
            let row_id = row.id.clone();
            let row_name = row.name.clone();
            this.clone().spawn_job_task(async move {
                if let Err(e) = this.run_row(row, "scheduled").await {
                    error!(
                        job_id = %row_id,
                        name = %row_name,
                        error = %e,
                        "{}", LogMsg::ScheduledRunFailed.text(this.log_lang())
                    );
                }
            });
        }

        Ok(())
    }
}

/// Заполняет `next_run_at` для задач, у которых он ещё не был рассчитан.
pub async fn init_job_schedules(pool: &DbPool, lang: LogLang) -> Result<(), sqlx::Error> {
    let rows: Vec<crate::models::JobRow> =
        sqlx::query_as("SELECT * FROM jobs WHERE next_run_at IS NULL")
            .fetch_all(pool)
            .await?;

    let count = rows.len();
    for row in rows {
        let st = match row.schedule_type.as_str() {
            "cron" => ScheduleType::Cron,
            "one_time" => ScheduleType::OneTime,
            _ => ScheduleType::Interval,
        };
        if let Some(next) = compute_next_run(&st, &row.schedule_value, None) {
            sqlx::query("UPDATE jobs SET next_run_at = ? WHERE id = ?")
                .bind(next)
                .bind(&row.id)
                .execute(pool)
                .await?;
        }
    }

    if count > 0 {
        info!(
            updated = count,
            "{}", LogMsg::SchedulesInitCount.text(lang)
        );
    }
    info!("{}", LogMsg::SchedulesInitialized.text(lang));
    Ok(())
}

/// Политика при старте процесса: отключить все задачи и/или не выполнять просроченные.
pub async fn apply_startup_policy(
    pool: &DbPool,
    config: &AppConfig,
    lang: LogLang,
) -> Result<(), sqlx::Error> {
    let updated_at = now_rfc3339();

    if config.disable_all_jobs_on_startup {
        let result = sqlx::query("UPDATE jobs SET enabled = 0, updated_at = ?")
            .bind(&updated_at)
            .execute(pool)
            .await?;
        info!(
            count = result.rows_affected(),
            "{}",
            LogMsg::StartupAllJobsDisabled.text(lang)
        );
    }

    if !config.run_overdue_on_startup {
        let now = now_rfc3339();
        let overdue: Vec<crate::models::JobRow> = sqlx::query_as(
            r#"
            SELECT * FROM jobs
            WHERE enabled = 1
              AND next_run_at IS NOT NULL
              AND next_run_at <= ?
            "#,
        )
        .bind(&now)
        .fetch_all(pool)
        .await?;

        let mut rescheduled = 0usize;
        for row in overdue {
            let st = schedule_type_from_str(&row.schedule_type);
            if let Some(next) = compute_next_run(&st, &row.schedule_value, None) {
                sqlx::query("UPDATE jobs SET next_run_at = ?, updated_at = ? WHERE id = ?")
                    .bind(&next)
                    .bind(&now_rfc3339())
                    .bind(&row.id)
                    .execute(pool)
                    .await?;
                rescheduled += 1;
            } else if st == ScheduleType::OneTime {
                sqlx::query("UPDATE jobs SET next_run_at = NULL, updated_at = ? WHERE id = ?")
                    .bind(&now_rfc3339())
                    .bind(&row.id)
                    .execute(pool)
                    .await?;
            }
        }

        if rescheduled > 0 {
            info!(
                count = rescheduled,
                "{}",
                LogMsg::StartupOverdueRescheduled.text(lang)
            );
        }
    }

    Ok(())
}

/// Вычисляет момент следующего запуска по типу и значению расписания.
pub fn compute_next_run(
    schedule_type: &ScheduleType,
    schedule_value: &str,
    _current_next: Option<&str>,
) -> Option<String> {
    let now = Utc::now();
    let next = match schedule_type {
        ScheduleType::Interval => next_interval(schedule_value, now)?,
        ScheduleType::Cron => next_cron(schedule_value, now)?,
        ScheduleType::OneTime => next_one_time(schedule_value, now)?,
    };
    Some(next.to_rfc3339())
}

/// После успешного выполнения возвращает следующий `next_run_at`; для one-time — `None`.
pub fn schedule_after_success(
    schedule_type: &str,
    schedule_value: &str,
    last_run: DateTime<Utc>,
) -> Option<String> {
    let st = match schedule_type {
        "cron" => ScheduleType::Cron,
        "one_time" => ScheduleType::OneTime,
        _ => ScheduleType::Interval,
    };

    if st == ScheduleType::OneTime {
        return None;
    }

    compute_next_run(&st, schedule_value, None).or_else(|| {
        if st == ScheduleType::Interval {
            parse_interval(schedule_value).map(|d| (last_run + d).to_rfc3339())
        } else {
            next_cron(schedule_value, last_run).map(|t| t.to_rfc3339())
        }
    })
}

/// Следующий запуск для интервального расписания: текущее время + интервал.
fn next_interval(value: &str, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let duration = parse_interval(value)?;
    Some(from + duration)
}

/// Разбирает строку интервала: `5m`, `2h`, `1d`, `10min`.
pub fn parse_interval(value: &str) -> Option<Duration> {
    let value = value.trim().to_lowercase();
    if let Some(num_str) = value.strip_suffix('m') {
        let n: i64 = num_str.parse().ok()?;
        return Some(Duration::minutes(n));
    }
    if let Some(num_str) = value.strip_suffix('h') {
        let n: i64 = num_str.parse().ok()?;
        return Some(Duration::hours(n));
    }
    if let Some(num_str) = value.strip_suffix('d') {
        let n: i64 = num_str.parse().ok()?;
        return Some(Duration::days(n));
    }
    if let Some(num_str) = value.strip_suffix("min") {
        let n: i64 = num_str.parse().ok()?;
        return Some(Duration::minutes(n));
    }
    None
}

/// Следующий запуск по cron-выражению.
fn next_cron(expression: &str, _from: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let schedule = Schedule::from_str(expression).ok()?;
    schedule.upcoming(Utc).next()
}

/// Однократный запуск: RFC3339 или `YYYY-MM-DD HH:MM:SS` (UTC); прошедшие даты игнорируются.
fn next_one_time(value: &str, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let at = DateTime::parse_from_rfc3339(value)
        .map(|d| d.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        })?;

    if at <= from {
        return None;
    }
    Some(at)
}

fn schedule_type_from_str(s: &str) -> ScheduleType {
    match s {
        "cron" => ScheduleType::Cron,
        "one_time" => ScheduleType::OneTime,
        _ => ScheduleType::Interval,
    }
}
