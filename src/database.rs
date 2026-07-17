//! Подключение к SQLite, миграции схемы и обслуживание журнала.

use crate::i18n::{LogLang, LogMsg};
use crate::models::{legacy_columns_to_steps, steps_to_json, JobRow};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::time::Duration;
use tracing::{debug, info};

/// Пул асинхронных соединений с базой данных.
pub type DbPool = SqlitePool;

/// Создаёт пул соединений, при необходимости создаёт файл БД и применяет миграции.
pub async fn init_pool(db_path: &str, lang: LogLang) -> Result<DbPool, sqlx::Error> {
    info!(db_path = %db_path, "{}", LogMsg::DbConnecting.text(lang));

    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await?;

    info!("{}", LogMsg::DbConnected.text(lang));
    run_migrations(&pool, lang).await?;
    Ok(pool)
}

/// Выполняет SQL-скрипты из каталога `migrations/` и ensure-миграции.
async fn run_migrations(pool: &DbPool, lang: LogLang) -> Result<(), sqlx::Error> {
    let sql = include_str!("../migrations/001_init.sql");
    let mut count = 0usize;
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement).execute(pool).await?;
        count += 1;
    }
    if ensure_execution_log_preview_truncated(pool).await? {
        count += 1;
    }
    if ensure_job_group_column(pool).await? {
        count += 1;
    }
    if ensure_jobs_steps_column(pool).await? {
        count += 1;
    }
    if ensure_execution_log_steps_log(pool).await? {
        count += 1;
    }
    migrate_legacy_steps_into_json(pool).await?;
    debug!(statements = count, "{}", LogMsg::MigrationsApplied.text(lang));
    info!("{}", LogMsg::MigrationsApplied.text(lang));
    Ok(())
}

async fn table_has_column(pool: &DbPool, table: &str, column: &str) -> Result<bool, sqlx::Error> {
    let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().any(|row| {
        row.try_get::<String, _>(1)
            .map(|name| name == column)
            .unwrap_or(false)
    }))
}

/// Добавляет колонку `preview_truncated`, если база создана до появления флага обрезки превью.
async fn ensure_execution_log_preview_truncated(pool: &DbPool) -> Result<bool, sqlx::Error> {
    if table_has_column(pool, "execution_logs", "preview_truncated").await? {
        return Ok(false);
    }
    sqlx::query(
        "ALTER TABLE execution_logs ADD COLUMN preview_truncated INTEGER NOT NULL DEFAULT 0",
    )
    .execute(pool)
    .await?;
    Ok(true)
}

/// Добавляет колонку `job_group` для косметической группировки в UI.
async fn ensure_job_group_column(pool: &DbPool) -> Result<bool, sqlx::Error> {
    if table_has_column(pool, "jobs", "job_group").await? {
        return Ok(false);
    }
    sqlx::query("ALTER TABLE jobs ADD COLUMN job_group TEXT")
        .execute(pool)
        .await?;
    Ok(true)
}

/// Добавляет JSON-колонку `steps` для упорядоченного пайплайна.
async fn ensure_jobs_steps_column(pool: &DbPool) -> Result<bool, sqlx::Error> {
    if table_has_column(pool, "jobs", "steps").await? {
        return Ok(false);
    }
    sqlx::query("ALTER TABLE jobs ADD COLUMN steps TEXT NOT NULL DEFAULT '[]'")
        .execute(pool)
        .await?;
    Ok(true)
}

/// Добавляет `steps_log` — полный JSON результатов шагов.
async fn ensure_execution_log_steps_log(pool: &DbPool) -> Result<bool, sqlx::Error> {
    if table_has_column(pool, "execution_logs", "steps_log").await? {
        return Ok(false);
    }
    sqlx::query("ALTER TABLE execution_logs ADD COLUMN steps_log TEXT")
        .execute(pool)
        .await?;
    Ok(true)
}

/// Конвертирует legacy fetch/transform/send в `steps`, если массив ещё пуст.
async fn migrate_legacy_steps_into_json(pool: &DbPool) -> Result<(), sqlx::Error> {
    let rows: Vec<JobRow> = sqlx::query_as(
        r#"
        SELECT * FROM jobs
        WHERE steps IS NULL OR TRIM(steps) = '' OR TRIM(steps) = '[]'
        "#,
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        let steps = legacy_columns_to_steps(
            row.fetch_enabled != 0,
            row.fetch_method.as_deref(),
            row.fetch_url.as_deref(),
            row.fetch_headers.as_deref(),
            row.fetch_body.as_deref(),
            row.transform_enabled != 0,
            row.transform_script.as_deref(),
            row.send_enabled != 0,
            row.send_method.as_deref(),
            row.send_url.as_deref(),
            row.send_headers.as_deref(),
            row.send_body_template.as_deref(),
        );
        if steps.is_empty() {
            continue;
        }
        let json = steps_to_json(&steps);
        sqlx::query("UPDATE jobs SET steps = ? WHERE id = ?")
            .bind(&json)
            .bind(&row.id)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Удаляет записи журнала старше указанного числа дней; возвращает число удалённых строк.
pub async fn purge_old_logs(
    pool: &DbPool,
    retention_days: u32,
    lang: LogLang,
) -> Result<u64, sqlx::Error> {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
    let cutoff_str = cutoff.to_rfc3339();
    let result = sqlx::query("DELETE FROM execution_logs WHERE started_at < ?")
        .bind(cutoff_str)
        .execute(pool)
        .await?;
    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            deleted = deleted,
            retention_days = retention_days,
            "{}", LogMsg::LogPurgeDone.text(lang)
        );
    }
    Ok(deleted)
}
