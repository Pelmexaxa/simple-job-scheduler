//! Подключение к SQLite, миграции схемы и обслуживание журнала.

use crate::i18n::{LogLang, LogMsg};
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

/// Выполняет SQL-скрипты из каталога `migrations/`.
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
    debug!(statements = count, "{}", LogMsg::MigrationsApplied.text(lang));
    info!("{}", LogMsg::MigrationsApplied.text(lang));
    Ok(())
}

/// Добавляет колонку `preview_truncated`, если база создана до появления флага обрезки превью.
async fn ensure_execution_log_preview_truncated(pool: &DbPool) -> Result<bool, sqlx::Error> {
    let rows = sqlx::query("PRAGMA table_info(execution_logs)")
        .fetch_all(pool)
        .await?;
    let has_column = rows.iter().any(|row| {
        row.try_get::<String, _>(1)
            .map(|name| name == "preview_truncated")
            .unwrap_or(false)
    });
    if has_column {
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
    let rows = sqlx::query("PRAGMA table_info(jobs)")
        .fetch_all(pool)
        .await?;
    let has_column = rows.iter().any(|row| {
        row.try_get::<String, _>(1)
            .map(|name| name == "job_group")
            .unwrap_or(false)
    });
    if has_column {
        return Ok(false);
    }
    sqlx::query("ALTER TABLE jobs ADD COLUMN job_group TEXT")
        .execute(pool)
        .await?;
    Ok(true)
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
