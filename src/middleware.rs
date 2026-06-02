//! HTTP-middleware: локализованное логирование входящих запросов.

use crate::api::AppState;
use crate::i18n::{LogLang, LogMsg};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Логирует метод, путь, статус и длительность каждого входящего запроса.
pub async fn log_requests(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let lang = LogLang::from_code(&state.config.default_language);
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let start = Instant::now();

    info!(
        method = %method,
        path = %path,
        "{}", LogMsg::ApiRequest.text(lang)
    );

    let response = next.run(request).await;
    let duration_ms = start.elapsed().as_millis();
    let status = response.status().as_u16();

    if status >= 500 {
        warn!(
            method = %method,
            path = %path,
            status = status,
            duration_ms = duration_ms,
            "{}", LogMsg::ApiError.text(lang)
        );
    } else {
        info!(
            method = %method,
            path = %path,
            status = status,
            duration_ms = duration_ms,
            "{}", LogMsg::ApiResponse.text(lang)
        );
    }

    response
}
