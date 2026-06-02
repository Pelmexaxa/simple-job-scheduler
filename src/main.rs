//! Точка входа: HTTP-сервер, планировщик, статика веб-интерфейса.

mod api;
mod config;
mod database;
mod execution;
mod logging;
mod i18n;
mod jobs;
mod middleware;
mod models;
mod scheduler;
mod validation;

use api::{AppState, router};
use axum::{Router, middleware as axum_middleware};
use config::AppConfig;
use database::init_pool;
use i18n::{LogMsg, format_config_summary};
use scheduler::{SchedulerHandle, apply_startup_policy, init_job_schedules};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tower_http::services::{ServeDir, ServeFile};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    // Конфигурация читается до инициализации tracing, чтобы задать уровень логов.
    let config = AppConfig::from_env();
    let lang = config.log_lang();

    logging::init_logging(&config).map_err(|e| {
        format!(
            "не удалось инициализировать файловые логи в {}: {e}",
            config.log_dir
        )
    })?;

    info!("{}", LogMsg::ServerStarting.text(lang));
    info!("{}", LogMsg::ConfigLoaded.text(lang));
    info!(
        "{}",
        format_config_summary(
            lang,
            &config.host,
            config.port,
            &config.db_path,
            &config.log_level,
            &config.default_language,
            &config.log_dir,
            config.max_concurrent_jobs,
            config.http_timeout_seconds,
            config.job_tick_interval_ms,
            config.enable_js_transform,
            config.retention_days,
            config.run_overdue_on_startup,
            config.disable_all_jobs_on_startup,
        )
    );

    let pool = init_pool(&config.db_path, lang).await?;
    init_job_schedules(&pool, lang).await?;
    apply_startup_policy(&pool, &config, lang).await?;

    let scheduler = SchedulerHandle::new(pool.clone(), config.clone());
    let scheduler_shutdown = scheduler.clone();
    scheduler.start_tick_loop();
    info!(
        tick_ms = config.job_tick_interval_ms,
        max_concurrent = config.max_concurrent_jobs,
        "{}",
        LogMsg::SchedulerStarted.text(lang)
    );

    let state = Arc::new(AppState {
        pool,
        scheduler,
        config: config.clone(),
    });

    let web_dir = ServeDir::new("web").not_found_service(ServeFile::new("web/index.html"));
    let i18n_dir = ServeDir::new("i18n");

    let api_routes = router(state.clone()).layer(axum_middleware::from_fn_with_state(
        state.clone(),
        middleware::log_requests,
    ));

    let app = Router::new()
        .merge(api_routes)
        .nest_service("/i18n", i18n_dir)
        .fallback_service(web_dir);

    let addr: SocketAddr = config.listen_addr().parse()?;
    info!(addr = %addr, "{}", LogMsg::ServerListening.text(lang));

    let listener = tokio::net::TcpListener::bind(addr).await?;

    let shutdown_lang = lang;
    let graceful = async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(
                error = %e,
                "{}",
                LogMsg::ShutdownSignalError.text(shutdown_lang)
            );
            return;
        }
        info!("{}", LogMsg::ShutdownRequested.text(shutdown_lang));
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(graceful)
        .await?;

    info!("{}", LogMsg::HttpServerStopped.text(lang));
    scheduler_shutdown.shutdown_and_drain().await;

    if tokio::time::timeout(StdDuration::from_secs(5), state.pool.close())
        .await
        .is_err()
    {
        warn!("{}", LogMsg::ShutdownPoolCloseTimeout.text(lang));
    }

    info!("{}", LogMsg::ShutdownComplete.text(lang));

    Ok(())
}
