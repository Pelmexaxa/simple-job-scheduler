//! CRUD задач, журнал выполнения и агрегаты для панели обзора.

use crate::database::DbPool;
use crate::models::{
    now_rfc3339, normalize_job_group, schedule_type_str, steps_to_json, DashboardStats,
    ExecutionLog, ExecutionLogRow, GroupEnabledResult, Job, JobInput, JobRow,
};
use crate::scheduler::compute_next_run;
use uuid::Uuid;

/// Возвращает все задачи с вычисленным состоянием и последним статусом выполнения.
pub async fn list_jobs(pool: &DbPool, running_ids: &[String]) -> Result<Vec<Job>, sqlx::Error> {
    let rows: Vec<JobRow> = sqlx::query_as("SELECT * FROM jobs ORDER BY created_at DESC")
        .fetch_all(pool)
        .await?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let last_status = last_log_status(pool, &row.id).await?;
        let running = running_ids.iter().any(|id| id == &row.id);
        jobs.push(row.into_job(last_status, running));
    }
    Ok(jobs)
}

/// Возвращает одну задачу по идентификатору или `None`.
pub async fn get_job(
    pool: &DbPool,
    id: &str,
    running: bool,
) -> Result<Option<Job>, sqlx::Error> {
    let row: Option<JobRow> = sqlx::query_as("SELECT * FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(r) => {
            let last_status = last_log_status(pool, &r.id).await?;
            Ok(Some(r.into_job(last_status, running)))
        }
        None => Ok(None),
    }
}

/// Создаёт задачу и рассчитывает первый `next_run_at`.
pub async fn create_job(pool: &DbPool, input: JobInput) -> Result<Job, sqlx::Error> {
    let id = Uuid::new_v4().to_string();
    let now = now_rfc3339();
    let next = compute_next_run(&input.schedule_type, &input.schedule_value, None);

    insert_job_row(pool, &id, &input, &now, next.as_deref()).await?;
    get_job(pool, &id, false)
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound)
}

/// Обновляет поля задачи и пересчитывает следующий запуск.
pub async fn update_job(pool: &DbPool, id: &str, input: JobInput) -> Result<Option<Job>, sqlx::Error> {
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    if exists.is_none() {
        return Ok(None);
    }

    let now = now_rfc3339();
    let existing: JobRow = sqlx::query_as("SELECT * FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_one(pool)
        .await?;

    let next = compute_next_run(
        &input.schedule_type,
        &input.schedule_value,
        existing.next_run_at.as_deref(),
    );

    let steps_json = steps_to_json(&input.steps);

    sqlx::query(
        r#"
        UPDATE jobs SET
            name = ?, description = ?, job_group = ?, enabled = ?,
            schedule_type = ?, schedule_value = ?,
            steps = ?,
            retry_enabled = ?, max_retries = ?, retry_interval_seconds = ?,
            updated_at = ?, next_run_at = ?
        WHERE id = ?
        "#,
    )
    .bind(&input.name)
    .bind(&input.description)
    .bind(normalize_job_group(input.job_group.clone()))
    .bind(i64::from(input.enabled))
    .bind(schedule_type_str(&input.schedule_type))
    .bind(&input.schedule_value)
    .bind(&steps_json)
    .bind(i64::from(input.retry_enabled))
    .bind(input.max_retries)
    .bind(input.retry_interval_seconds)
    .bind(&now)
    .bind(next)
    .bind(id)
    .execute(pool)
    .await?;

    get_job(pool, id, false).await
}

/// Удаляет задачу; возвращает `true`, если строка была удалена.
pub async fn delete_job(pool: &DbPool, id: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM jobs WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Возвращает активные задачи, у которых наступило время `next_run_at`.
pub async fn due_jobs(pool: &DbPool, now: &str) -> Result<Vec<JobRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT * FROM jobs
        WHERE enabled = 1
          AND next_run_at IS NOT NULL
          AND next_run_at <= ?
        ORDER BY next_run_at ASC
        "#,
    )
    .bind(now)
    .fetch_all(pool)
    .await
}

/// Сдвигает `next_run_at` вперёд при старте планового запуска, пока задача ещё «просрочена».
pub async fn claim_next_run(
    pool: &DbPool,
    id: &str,
    now: &str,
    next_run: &str,
) -> Result<bool, sqlx::Error> {
    let updated = now_rfc3339();
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET next_run_at = ?, updated_at = ?
        WHERE id = ?
          AND enabled = 1
          AND next_run_at IS NOT NULL
          AND next_run_at <= ?
        "#,
    )
    .bind(next_run)
    .bind(&updated)
    .bind(id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Обновляет метки последнего и следующего запуска после выполнения.
pub async fn update_run_times(
    pool: &DbPool,
    id: &str,
    last_run: &str,
    next_run: Option<String>,
) -> Result<(), sqlx::Error> {
    let now = now_rfc3339();
    sqlx::query(
        "UPDATE jobs SET last_run_at = ?, next_run_at = ?, updated_at = ? WHERE id = ?",
    )
    .bind(last_run)
    .bind(next_run)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Возвращает последние записи журнала для указанной задачи.
pub async fn job_logs(pool: &DbPool, job_id: &str, limit: i64) -> Result<Vec<ExecutionLog>, sqlx::Error> {
    let rows: Vec<ExecutionLogRow> = sqlx::query_as(
        "SELECT * FROM execution_logs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?",
    )
    .bind(job_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.into_log()).collect())
}

/// Сохраняет запись о выполнении задачи.
pub async fn insert_log(
    pool: &DbPool,
    log: &ExecutionLogRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO execution_logs (
            id, job_id, started_at, finished_at, status,
            fetch_status, send_status, duration_ms, error_message, response_preview,
            preview_truncated, steps_log
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&log.id)
    .bind(&log.job_id)
    .bind(&log.started_at)
    .bind(&log.finished_at)
    .bind(&log.status)
    .bind(log.fetch_status)
    .bind(log.send_status)
    .bind(log.duration_ms)
    .bind(&log.error_message)
    .bind(&log.response_preview)
    .bind(log.preview_truncated)
    .bind(&log.steps_log)
    .execute(pool)
    .await?;
    Ok(())
}

/// Собирает счётчики и последние записи журнала для дашборда.
pub async fn dashboard(pool: &DbPool) -> Result<DashboardStats, sqlx::Error> {
    let active_jobs: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE enabled = 1")
            .fetch_one(pool)
            .await?;
    let paused_jobs: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE enabled = 0")
            .fetch_one(pool)
            .await?;

    let failed_jobs: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(DISTINCT job_id) FROM execution_logs
        WHERE status = 'failed'
          AND started_at = (
            SELECT MAX(started_at) FROM execution_logs el2 WHERE el2.job_id = execution_logs.job_id
          )
        "#,
    )
    .fetch_one(pool)
    .await?;

    let total_executions: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM execution_logs")
            .fetch_one(pool)
            .await?;
    let succeeded_executions: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM execution_logs WHERE status = 'succeeded'",
    )
    .fetch_one(pool)
    .await?;
    let failed_executions: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM execution_logs WHERE status = 'failed'",
    )
    .fetch_one(pool)
    .await?;

    let recent_rows: Vec<ExecutionLogRow> = sqlx::query_as(
        "SELECT * FROM execution_logs ORDER BY started_at DESC LIMIT 10",
    )
    .fetch_all(pool)
    .await?;

    Ok(DashboardStats {
        active_jobs: active_jobs.0,
        paused_jobs: paused_jobs.0,
        failed_jobs: failed_jobs.0,
        total_executions: total_executions.0,
        succeeded_executions: succeeded_executions.0,
        failed_executions: failed_executions.0,
        recent_logs: recent_rows.into_iter().map(|r| r.into_log()).collect(),
    })
}

/// Вставляет новую строку в таблицу `jobs`.
async fn insert_job_row(
    pool: &DbPool,
    id: &str,
    input: &JobInput,
    now: &str,
    next_run: Option<&str>,
) -> Result<(), sqlx::Error> {
    let steps_json = steps_to_json(&input.steps);
    sqlx::query(
        r#"
        INSERT INTO jobs (
            id, name, description, job_group, enabled,
            schedule_type, schedule_value, steps,
            fetch_enabled, transform_enabled, send_enabled,
            retry_enabled, max_retries, retry_interval_seconds,
            created_at, updated_at, last_run_at, next_run_at
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            0, 0, 0,
            ?, ?, ?,
            ?, ?, NULL, ?
        )
        "#,
    )
    .bind(id)
    .bind(&input.name)
    .bind(&input.description)
    .bind(normalize_job_group(input.job_group.clone()))
    .bind(i64::from(input.enabled))
    .bind(schedule_type_str(&input.schedule_type))
    .bind(&input.schedule_value)
    .bind(&steps_json)
    .bind(i64::from(input.retry_enabled))
    .bind(input.max_retries)
    .bind(input.retry_interval_seconds)
    .bind(now)
    .bind(now)
    .bind(next_run)
    .execute(pool)
    .await?;
    Ok(())
}

/// Возвращает статус последнего выполнения задачи (`succeeded` / `failed`).
async fn last_log_status(pool: &DbPool, job_id: &str) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM execution_logs WHERE job_id = ? ORDER BY started_at DESC LIMIT 1",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

/// Включает или отключает все задачи с указанной группой (точное совпадение после trim).
pub async fn set_group_enabled(
    pool: &DbPool,
    job_group: &str,
    enabled: bool,
) -> Result<GroupEnabledResult, sqlx::Error> {
    let group = job_group.trim();
    if group.is_empty() {
        return Ok(GroupEnabledResult { updated: 0 });
    }
    let now = now_rfc3339();
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET enabled = ?, updated_at = ?
        WHERE TRIM(COALESCE(job_group, '')) = ?
        "#,
    )
    .bind(i64::from(enabled))
    .bind(&now)
    .bind(group)
    .execute(pool)
    .await?;
    Ok(GroupEnabledResult {
        updated: result.rows_affected(),
    })
}
