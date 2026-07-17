//! REST API планировщика (префикс `/api`).

use crate::config::AppConfig;
use crate::database::DbPool;
use crate::i18n::{LogLang, LogMsg};
use crate::jobs;
use crate::models::{GroupEnabledInput, JobInput, PublicSettings};
use crate::scheduler::SchedulerHandle;
use crate::validation;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;
use tracing::{info, warn};

/// Общее состояние HTTP-обработчиков: БД, планировщик и конфигурация.
#[derive(Clone)]
pub struct AppState {
    pub pool: DbPool,
    pub scheduler: SchedulerHandle,
    pub config: AppConfig,
}

impl AppState {
    fn log_lang(&self) -> LogLang {
        self.config.log_lang()
    }
}

/// Собирает маршруты API и подключает общее состояние.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/dashboard", get(dashboard))
        .route("/api/settings", get(settings))
        .route("/api/jobs", get(list_jobs).post(create_job))
        .route("/api/jobs/group-enabled", post(set_group_enabled))
        .route(
            "/api/jobs/{id}",
            put(update_job).delete(delete_job),
        )
        .route("/api/jobs/{id}/run", post(run_job))
        .route("/api/jobs/{id}/logs", get(job_logs))
        .with_state(state)
}

/// GET `/api/settings` — публичные параметры для веб-интерфейса.
async fn settings(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let _ = state;
    Json(PublicSettings {
        max_step_output_bytes: u32::try_from(crate::execution::MAX_STEP_OUTPUT_BYTES)
            .unwrap_or(u32::MAX),
    })
    .into_response()
}

/// GET `/api/dashboard` — статистика и последние запуски.
async fn dashboard(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match jobs::dashboard(&state.pool).await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => {
            warn!(error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// GET `/api/jobs` — список всех задач.
async fn list_jobs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let running = state.scheduler.running_ids().await;
    match jobs::list_jobs(&state.pool, &running).await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(e) => {
            warn!(error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/jobs` — создание задачи.
async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(input): Json<JobInput>,
) -> impl IntoResponse {
    let lang = state.log_lang();
    if let Err(error) = validation::validate_job(&input, lang) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error })),
        )
            .into_response();
    }

    match jobs::create_job(&state.pool, input).await {
        Ok(job) => {
            info!(
                job_id = %job.id,
                name = %job.name,
                "{}", LogMsg::JobCreated.text(state.log_lang())
            );
            (StatusCode::CREATED, Json(job)).into_response()
        }
        Err(e) => {
            warn!(error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// PUT `/api/jobs/:id` — обновление задачи.
async fn update_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<JobInput>,
) -> impl IntoResponse {
    let lang = state.log_lang();
    if let Err(error) = validation::validate_job(&input, lang) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error })),
        )
            .into_response();
    }

    match jobs::update_job(&state.pool, &id, input).await {
        Ok(Some(job)) => {
            info!(
                job_id = %job.id,
                name = %job.name,
                "{}", LogMsg::JobUpdated.text(state.log_lang())
            );
            Json(job).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "задача не найдена" })),
        )
            .into_response(),
        Err(e) => {
            warn!(job_id = %id, error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// DELETE `/api/jobs/:id` — удаление задачи.
async fn delete_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match jobs::delete_job(&state.pool, &id).await {
        Ok(true) => {
            info!(job_id = %id, "{}", LogMsg::JobDeleted.text(state.log_lang()));
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "задача не найдена" })),
        )
            .into_response(),
        Err(e) => {
            warn!(job_id = %id, error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/jobs/group-enabled` — включить или отключить все задачи группы.
async fn set_group_enabled(
    State(state): State<Arc<AppState>>,
    Json(input): Json<GroupEnabledInput>,
) -> impl IntoResponse {
    let group = input.job_group.trim();
    if group.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "job_group is required" })),
        )
            .into_response();
    }

    match jobs::set_group_enabled(&state.pool, group, input.enabled).await {
        Ok(result) => Json(result).into_response(),
        Err(e) => {
            warn!(error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/jobs/:id/run` — немедленный запуск задачи в фоне.
async fn run_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let exists = jobs::get_job(&state.pool, &id, false).await;
    match exists {
        Ok(Some(_)) => {
            info!(job_id = %id, "{}", LogMsg::JobRunQueued.text(state.log_lang()));
            state.scheduler.spawn_manual(id.clone()).await;
            (
                StatusCode::ACCEPTED,
                Json(json!({ "message": "запуск задачи начат", "job_id": id })),
            )
                .into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "задача не найдена" })),
        )
            .into_response(),
        Err(e) => {
            warn!(job_id = %id, error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// GET `/api/jobs/:id/logs` — журнал выполнения задачи.
async fn job_logs(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match jobs::job_logs(&state.pool, &id, 100).await {
        Ok(logs) => Json(logs).into_response(),
        Err(e) => {
            warn!(job_id = %id, error = %e, "{}", LogMsg::ApiError.text(state.log_lang()));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}
