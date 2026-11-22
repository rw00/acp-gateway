use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::VERSION;
use crate::config::expand_tilde;
use crate::models::{ContinueSessionRequest, StartSessionRequest};
use crate::runner::SessionManager;

const CHUNK_PROGRESS_INTERVAL: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct McpState {
    pub manager: Arc<SessionManager>,
    sessions: Arc<Mutex<HashSet<String>>>,
}

impl McpState {
    pub fn new(manager: Arc<SessionManager>) -> Self {
        Self {
            manager,
            sessions: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

pub async fn mcp_get() -> StatusCode {
    StatusCode::METHOD_NOT_ALLOWED
}

pub async fn mcp_delete(State(state): State<McpState>, headers: HeaderMap) -> StatusCode {
    if let Some(sid) = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
    {
        let removed = state
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(sid);
        if removed {
            return StatusCode::OK;
        }
    }
    StatusCode::NOT_FOUND
}

pub async fn mcp_post(
    State(state): State<McpState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let accept_sse = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/event-stream"));

    let msg: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_rpc_response(
                json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32700, "message": format!("Parse error: {e}") }
                }),
                None,
                false,
            );
        }
    };

    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    let req_id = msg.get("id").cloned();
    let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));

    if method == "initialize" {
        let session_id = Uuid::new_v4().to_string();
        state
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id.clone());

        let protocol_version = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .unwrap_or("2025-03-26");

        let reply = json!({
            "jsonrpc": "2.0",
            "id": req_id.unwrap_or(Value::Null),
            "result": {
                "protocolVersion": protocol_version,
                "capabilities": {
                    "tools": { "listChanged": false }
                },
                "serverInfo": {
                    "name": "acp-gateway",
                    "version": VERSION
                }
            }
        });
        return json_rpc_response(reply, Some(&session_id), accept_sse);
    }

    // Notifications (no id)
    if req_id.is_none() {
        return StatusCode::ACCEPTED.into_response();
    }
    let req_id = req_id.unwrap();

    // Validate session header if provided
    if let Some(sid) = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
    {
        let known = state
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(sid);
        if !known {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": { "code": -32001, "message": "Unknown mcp-session-id" }
                })),
            )
                .into_response();
        }
    }

    match method {
        "ping" => json_rpc_response(
            json!({ "jsonrpc": "2.0", "id": req_id, "result": {} }),
            None,
            accept_sse,
        ),
        "tools/list" => json_rpc_response(
            json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": { "tools": tool_definitions() }
            }),
            None,
            accept_sse,
        ),
        "tools/call" => handle_tools_call(state.manager, req_id, params, accept_sse).await,
        other => json_rpc_response(
            json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": { "code": -32601, "message": format!("Method not found: {other}") }
            }),
            None,
            accept_sse,
        ),
    }
}

async fn handle_tools_call(
    manager: Arc<SessionManager>,
    req_id: Value,
    params: Value,
    accept_sse: bool,
) -> Response {
    let tool_name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let progress_token = params.pointer("/_meta/progressToken").cloned();

    if !accept_sse {
        let result = execute_tool(manager, &tool_name, arguments, None).await;
        return json_rpc_response(
            json!({ "jsonrpc": "2.0", "id": req_id, "result": result }),
            None,
            false,
        );
    }

    let (sse_tx, sse_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        let (update_tx, update_rx) = mpsc::unbounded_channel::<Value>();
        let progress_handle = if let Some(token) = progress_token {
            let tx = sse_tx.clone();
            Some(tokio::spawn(async move {
                forward_progress(update_rx, token, tx).await;
            }))
        } else {
            drop(update_rx);
            None
        };

        let maybe_update_tx = if progress_handle.is_some() {
            Some(update_tx)
        } else {
            None
        };

        let result = execute_tool(manager, &tool_name, arguments, maybe_update_tx).await;

        if let Some(h) = progress_handle {
            let _ = h.await;
        }

        let final_msg = json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": result,
        });
        let _ = sse_tx.send(format_mcp_sse(&final_msg));
    });

    let stream = UnboundedReceiverStream::new(sse_rx).map(Ok::<String, Infallible>);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn forward_progress(
    mut update_rx: mpsc::UnboundedReceiver<Value>,
    progress_token: Value,
    sse_tx: mpsc::UnboundedSender<String>,
) {
    let mut step: f64 = 0.0;
    let mut last_chunk_report: Option<Instant> = None;

    loop {
        match tokio::time::timeout(HEARTBEAT_INTERVAL, update_rx.recv()).await {
            Ok(Some(update)) => {
                if let Some(msg) = format_progress_message(&update, &mut last_chunk_report) {
                    step += 1.0;
                    let note = json!({
                        "jsonrpc": "2.0",
                        "method": "notifications/progress",
                        "params": {
                            "progressToken": progress_token,
                            "progress": step,
                            "message": msg,
                        }
                    });
                    let _ = sse_tx.send(format_mcp_sse(&note));
                }
            }
            Ok(None) => break,
            Err(_) => {
                step += 1.0;
                let note = json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/progress",
                    "params": {
                        "progressToken": progress_token,
                        "progress": step,
                        "message": "working...",
                    }
                });
                let _ = sse_tx.send(format_mcp_sse(&note));
            }
        }
    }
}

fn format_progress_message(update: &Value, last_chunk_report: &mut Option<Instant>) -> Option<String> {
    let kind = update.get("sessionUpdate").and_then(Value::as_str)?;
    match kind {
        "agent_message_chunk" | "agent_thought_chunk" => {
            let now = Instant::now();
            if let Some(last) = last_chunk_report {
                if now.duration_since(*last) < CHUNK_PROGRESS_INTERVAL {
                    return None;
                }
            }
            *last_chunk_report = Some(now);
            Some(if kind == "agent_thought_chunk" {
                "thinking...".into()
            } else {
                "streaming response...".into()
            })
        }
        "tool_call" => {
            let title = update
                .get("title")
                .and_then(Value::as_str)
                .or_else(|| update.get("name").and_then(Value::as_str))
                .unwrap_or("tool");
            Some(format!("tool_call: {title}"))
        }
        "tool_call_update" => {
            let status = update.get("status").and_then(Value::as_str)?;
            if status == "completed" || status == "failed" {
                let title = update
                    .get("title")
                    .and_then(Value::as_str)
                    .or_else(|| update.get("toolCallId").and_then(Value::as_str))
                    .unwrap_or("tool");
                Some(format!("tool_{status}: {title}"))
            } else {
                None
            }
        }
        "plan" => Some("plan updated".into()),
        "usage_update" => {
            let used = update.get("used").and_then(Value::as_u64);
            let size = update.get("size").and_then(Value::as_u64);
            match (used, size) {
                (Some(u), Some(s)) => Some(format!("usage: {u}/{s}")),
                _ => Some("usage_update".into()),
            }
        }
        _ => None,
    }
}

async fn execute_tool(
    manager: Arc<SessionManager>,
    name: &str,
    args: Value,
    on_update: Option<mpsc::UnboundedSender<Value>>,
) -> Value {
    match name {
        "acp_start" => {
            let prompt = args
                .get("prompt")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let folder_arg = args
                .get("folder")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();

            let target_folder = if !folder_arg.is_empty() {
                folder_arg
            } else if let Some(first) = manager.settings.allowed_base_dirs.first() {
                let exp = expand_tilde(first);
                std::fs::canonicalize(&exp)
                    .unwrap_or(exp)
                    .to_string_lossy()
                    .to_string()
            } else {
                return tool_error(
                    "`folder` is required when no allowed_base_dirs are configured.",
                );
            };

            let req = StartSessionRequest {
                folder: target_folder,
                prompt,
                model: opt_non_empty_str(&args, "model"),
                effort: opt_non_empty_str(&args, "effort"),
                autonomous: args.get("autonomous").and_then(Value::as_bool),
                sandbox: args.get("sandbox").and_then(Value::as_bool),
                timeout_seconds: args.get("timeout_seconds").and_then(Value::as_u64),
                stream: false,
            };
            if let Err(e) = req.validate() {
                return tool_error(&e);
            }
            match manager.start_session(req, None, on_update).await {
                Ok(res) => tool_ok(serde_json::to_value(res).unwrap_or(Value::Null)),
                Err(e) => tool_error(&e.to_string()),
            }
        }
        "acp_continue" => {
            let conversation_id = args
                .get("conversation_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            let req = ContinueSessionRequest {
                prompt: args
                    .get("prompt")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                model: opt_non_empty_str(&args, "model"),
                effort: opt_non_empty_str(&args, "effort"),
                timeout_seconds: args.get("timeout_seconds").and_then(Value::as_u64),
                stream: false,
            };
            if let Err(e) = req.validate() {
                return tool_error(&e);
            }
            match manager
                .continue_session(
                    conversation_id,
                    &req.prompt,
                    req.model.as_deref(),
                    req.effort.as_deref(),
                    req.timeout_seconds,
                    None,
                    on_update,
                )
                .await
            {
                Ok(res) => tool_ok(serde_json::to_value(res).unwrap_or(Value::Null)),
                Err(e) => tool_error(&e.to_string()),
            }
        }
        "acp_list_sessions" => {
            let sessions = manager.list_sessions();
            tool_ok(serde_json::to_value(sessions).unwrap_or_else(|_| json!([])))
        }
        "acp_get_session" => {
            let conversation_id = args
                .get("conversation_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            match manager.get_session(conversation_id) {
                Ok(s) => tool_ok(serde_json::to_value(s).unwrap_or(Value::Null)),
                Err(e) => tool_error(&e.to_string()),
            }
        }
        "acp_cancel" => {
            let conversation_id = args
                .get("conversation_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            match manager.cancel_session(conversation_id).await {
                Ok(stopped) => tool_ok(json!({
                    "conversation_id": conversation_id,
                    "stopped": stopped,
                })),
                Err(e) => tool_error(&e.to_string()),
            }
        }
        "acp_models" => match manager.list_models().await {
            Ok(models) => tool_ok(serde_json::to_value(models).unwrap_or_else(|_| json!([]))),
            Err(e) => tool_error(&e.to_string()),
        },
        other => tool_error(&format!("Unknown tool: {other}")),
    }
}

fn opt_non_empty_str(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn tool_ok(payload: Value) -> Value {
    let text = serde_json::to_string(&payload).unwrap_or_else(|_| "{}".into());
    json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": payload,
        "isError": false,
    })
}

fn tool_error(msg: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": msg }],
        "isError": true,
    })
}

fn format_mcp_sse(payload: &Value) -> String {
    format!(
        "event: message\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_else(|_| "{}".into())
    )
}

fn json_rpc_response(payload: Value, session_id: Option<&str>, as_sse: bool) -> Response {
    let mut builder = Response::builder().status(StatusCode::OK);
    if let Some(sid) = session_id {
        if let Ok(val) = HeaderValue::from_str(sid) {
            builder = builder.header("mcp-session-id", val);
        }
    }
    if as_sse {
        builder
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(format_mcp_sse(&payload)))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    } else {
        builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_string(&payload).unwrap_or_else(|_| "{}".into()),
            ))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

fn tool_definitions() -> Value {
    json!([
        {
            "name": "acp_start",
            "description": "Start a new ACP agent conversation with access to `folder`.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "prompt": { "type": "string" },
                    "folder": { "type": "string" },
                    "model": { "type": "string" },
                    "effort": { "type": "string" },
                    "autonomous": { "type": "boolean" },
                    "sandbox": { "type": "boolean" },
                    "timeout_seconds": { "type": "integer", "minimum": 0 }
                },
                "required": ["prompt"]
            }
        },
        {
            "name": "acp_continue",
            "description": "Continue an existing ACP agent conversation by `conversation_id`.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "conversation_id": { "type": "string" },
                    "prompt": { "type": "string" },
                    "model": { "type": "string" },
                    "effort": { "type": "string" },
                    "timeout_seconds": { "type": "integer", "minimum": 0 }
                },
                "required": ["conversation_id", "prompt"]
            }
        },
        {
            "name": "acp_list_sessions",
            "description": "List all tracked ACP conversations and their warm/busy status.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "acp_get_session",
            "description": "Get metadata and status for a specific ACP conversation.",
            "inputSchema": {
                "type": "object",
                "properties": { "conversation_id": { "type": "string" } },
                "required": ["conversation_id"]
            }
        },
        {
            "name": "acp_cancel",
            "description": "Stop the active session for a conversation (can still be resumed later).",
            "inputSchema": {
                "type": "object",
                "properties": { "conversation_id": { "type": "string" } },
                "required": ["conversation_id"]
            }
        },
        {
            "name": "acp_models",
            "description": "List available models exposed by the configured ACP agent.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}
