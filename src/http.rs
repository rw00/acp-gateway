use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::config::Settings;
use crate::mcp::{McpState, mcp_delete, mcp_get, mcp_post};
use crate::models::{ContinueSessionRequest, StartSessionRequest};
use crate::openai;
use crate::runner::{GatewayError, SessionManager};

const LOCAL_HOSTS: &[&str] = &[
    "127.0.0.1",
    "localhost",
    "::1",
    "[::1]",
    "host.docker.internal",
    "test",
    "testserver",
];
const SSE_KEEPALIVE_SECONDS: u64 = 15;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) manager: Arc<SessionManager>,
    is_loopback: bool,
    api_token: Option<String>,
}

pub fn create_app(settings: Settings) -> (Router, Arc<SessionManager>) {
    let is_loopback = matches!(settings.host.as_str(), "127.0.0.1" | "localhost" | "::1");
    if !is_loopback && settings.api_token.is_none() {
        tracing::warn!(
            "Binding to non-loopback host {} without ACP_GATEWAY_API_TOKEN; endpoints are unauthenticated",
            settings.host
        );
    }

    let mcp_path = settings.mcp_path.clone();
    let api_token = settings.api_token.clone();
    let manager = SessionManager::new(settings).expect("failed to initialize SessionManager");

    let app_state = AppState {
        manager: manager.clone(),
        is_loopback,
        api_token,
    };
    let mcp_state = McpState::new(manager.clone());

    let mcp_router = Router::new()
        .route(&mcp_path, post(mcp_post).get(mcp_get).delete(mcp_delete))
        .with_state(mcp_state);

    let rest_router = Router::new()
        .route("/health", get(health))
        .route("/models", get(list_models))
        .route("/v1/models", get(openai::list_models))
        .route("/v1/chat/completions", post(openai::chat_completions))
        .route("/sessions", get(list_sessions).post(start_session))
        .route("/sessions/{conversation_id}", get(get_session))
        .route(
            "/sessions/{conversation_id}/messages",
            post(continue_session),
        )
        .route("/sessions/{conversation_id}/cancel", post(cancel_session))
        .with_state(app_state.clone());

    let router = rest_router
        .merge(mcp_router)
        .layer(middleware::from_fn_with_state(
            app_state,
            security_middleware,
        ));

    (router, manager)
}

async fn security_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    if state.is_loopback {
        let host_hdr = req
            .headers()
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if host_hdr.is_empty() || !is_local_host(&extract_hostname(host_hdr)) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "detail": "Forbidden Host header" })),
            )
                .into_response();
        }

        if let Some(origin_val) = req.headers().get(header::ORIGIN) {
            let origin_str = origin_val.to_str().unwrap_or("").trim();
            if !origin_str.is_empty() && !is_valid_local_origin(origin_str) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "detail": "Forbidden Origin header" })),
                )
                    .into_response();
            }
        }
    }

    if let Some(ref expected) = state.api_token {
        if req.uri().path() != "/health" {
            let auth_bytes = req
                .headers()
                .get(header::AUTHORIZATION)
                .map(|v| v.as_bytes())
                .unwrap_or(b"");
            let token_bytes = auth_bytes.strip_prefix(b"Bearer ").unwrap_or(b"");
            if !constant_time_eq(token_bytes, expected.as_bytes()) {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "detail": "Unauthorized" })),
                )
                    .into_response();
            }
        }
    }

    next.run(req).await
}

fn extract_hostname(host_header: &str) -> String {
    let host = host_header.trim().to_ascii_lowercase();
    if host.starts_with('[') {
        if let Some(end) = host.find(']') {
            return host[..=end].to_string();
        }
        return host;
    }
    host.split(':').next().unwrap_or("").to_string()
}

fn is_local_host(host: &str) -> bool {
    LOCAL_HOSTS.contains(&host)
}

fn is_valid_local_origin(origin: &str) -> bool {
    let rest = if let Some(r) = origin.strip_prefix("http://") {
        r
    } else if let Some(r) = origin.strip_prefix("https://") {
        r
    } else {
        return false;
    };
    let netloc = rest.split('/').next().unwrap_or("");
    if netloc.is_empty() {
        return false;
    }
    is_local_host(&extract_hostname(netloc))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn map_error(err: GatewayError) -> Response {
    let status = match &err {
        GatewayError::FolderAccess(_) => StatusCode::BAD_REQUEST,
        GatewayError::SessionNotFound(_) => StatusCode::NOT_FOUND,
        GatewayError::TurnTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
        GatewayError::Gateway { .. } => StatusCode::BAD_GATEWAY,
    };
    (status, Json(json!({ "detail": err.to_string() }))).into_response()
}

fn format_sse(event_name: &str, data: &Value) -> String {
    let safe_event: String = event_name
        .chars()
        .filter(|&c| c != '\r' && c != '\n')
        .collect();
    let safe_event = if safe_event.is_empty() {
        "message"
    } else {
        safe_event.as_str()
    };
    format!(
        "event: {safe_event}\ndata: {}\n\n",
        serde_json::to_string(data).unwrap_or_else(|_| "{}".into())
    )
}

fn stream_turn<F, Fut>(work: F) -> Response
where
    F: FnOnce(mpsc::UnboundedSender<Value>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), GatewayError>> + Send + 'static,
{
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Value>();
    let (out_tx, out_rx) = mpsc::unbounded_channel::<String>();

    let worker_out = out_tx.clone();
    let task = tokio::spawn(async move {
        if let Err(err) = work(ev_tx).await {
            let _ = worker_out.send(format_sse("error", &json!({ "error": err.to_string() })));
        }
    });

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = out_tx.closed() => {
                    task.abort();
                    break;
                }
                recv = tokio::time::timeout(Duration::from_secs(SSE_KEEPALIVE_SECONDS), ev_rx.recv()) => {
                    match recv {
                        Ok(Some(ev)) => {
                            let name = ev.get("event").and_then(Value::as_str).unwrap_or("message");
                            if out_tx.send(format_sse(name, &ev)).is_err() {
                                task.abort();
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            if out_tx.send(": keepalive\n\n".to_string()).is_err() {
                                task.abort();
                                break;
                            }
                        }
                    }
                }
            }
        }
    });

    let stream = UnboundedReceiverStream::new(out_rx).map(Ok::<String, Infallible>);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_models(State(state): State<AppState>) -> Response {
    match state.manager.list_models().await {
        Ok(models) => Json(models).into_response(),
        Err(err) => map_error(err),
    }
}

async fn list_sessions(State(state): State<AppState>) -> Json<Value> {
    Json(serde_json::to_value(state.manager.list_sessions()).unwrap_or_else(|_| json!([])))
}

async fn get_session(
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
) -> Response {
    match state.manager.get_session(&conversation_id) {
        Ok(s) => Json(s).into_response(),
        Err(err) => map_error(err),
    }
}

async fn start_session(
    State(state): State<AppState>,
    Json(req): Json<StartSessionRequest>,
) -> Response {
    if let Err(msg) = req.validate() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "detail": msg })),
        )
            .into_response();
    }

    if req.stream {
        if let Err(err) = state.manager.validate_folder(&req.folder) {
            return map_error(err);
        }
        let manager = state.manager.clone();
        return stream_turn(move |ev_tx| async move {
            manager.start_session(req, Some(ev_tx), None).await?;
            Ok(())
        });
    }

    match state.manager.start_session(req, None, None).await {
        Ok(res) => Json(res).into_response(),
        Err(err) => map_error(err),
    }
}

async fn continue_session(
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
    Json(req): Json<ContinueSessionRequest>,
) -> Response {
    if let Err(msg) = req.validate() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "detail": msg })),
        )
            .into_response();
    }

    if req.stream {
        match state.manager.get_session(&conversation_id) {
            Ok(session) => {
                if let Err(err) = state.manager.validate_folder(&session.folder) {
                    return map_error(err);
                }
            }
            Err(err) => return map_error(err),
        }
        let manager = state.manager.clone();
        return stream_turn(move |ev_tx| async move {
            manager
                .continue_session(
                    &conversation_id,
                    &req.prompt,
                    req.model.as_deref(),
                    req.effort.as_deref(),
                    req.timeout_seconds,
                    Some(ev_tx),
                    None,
                )
                .await?;
            Ok(())
        });
    }

    match state
        .manager
        .continue_session(
            &conversation_id,
            &req.prompt,
            req.model.as_deref(),
            req.effort.as_deref(),
            req.timeout_seconds,
            None,
            None,
        )
        .await
    {
        Ok(res) => Json(res).into_response(),
        Err(err) => map_error(err),
    }
}

async fn cancel_session(
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
) -> Response {
    match state.manager.cancel_session(&conversation_id).await {
        Ok(stopped) => Json(json!({
            "conversation_id": conversation_id,
            "stopped": stopped,
        }))
        .into_response(),
        Err(err) => map_error(err),
    }
}
