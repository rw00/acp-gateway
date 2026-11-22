use std::convert::Infallible;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::http::AppState;
use crate::runner::{CompletionStreamEvent, GatewayError};

const SSE_KEEPALIVE_SECONDS: u64 = 15;

const STATELESS_COMPLETION_PREAMBLE: &str = "\
[STATELESS COMPLETION MODE]\n\
You are operating strictly as a stateless LLM completion backend for an external orchestrator.\n\
- Do NOT invoke any built-in tools (no shell commands, file reads/writes, web search, or subagents).\n\
- Follow the <system> instructions below as your sole instructions and required output format.\n\
- Reply directly with only the assistant's next response to the conversation below.\n\n";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChatCompletionRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub stream_options: Option<StreamOptions>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StreamOptions {
    #[serde(default)]
    pub include_usage: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ChatMessage {
    pub role: String,
    #[serde(default)]
    pub content: Option<MessageContent>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ContentPart {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub image_url: Option<ImageUrl>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum ImageUrl {
    Url(String),
    Obj { url: String },
}

impl ImageUrl {
    fn as_str(&self) -> &str {
        match self {
            ImageUrl::Url(s) => s.as_str(),
            ImageUrl::Obj { url } => url.as_str(),
        }
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn openai_error_response(status: StatusCode, err_type: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message.into(),
                "type": err_type,
                "param": Value::Null,
                "code": Value::Null,
            }
        })),
    )
        .into_response()
}

fn map_openai_error(err: GatewayError) -> Response {
    let (status, err_type) = match &err {
        GatewayError::FolderAccess(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
        GatewayError::SessionNotFound(_) => (StatusCode::NOT_FOUND, "invalid_request_error"),
        GatewayError::TurnTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout_error"),
        GatewayError::Gateway { .. } => (StatusCode::BAD_GATEWAY, "server_error"),
    };
    openai_error_response(status, err_type, err.to_string())
}

fn map_finish_reason(stop_reason: &str) -> &'static str {
    match stop_reason {
        "max_tokens" | "length" => "length",
        _ => "stop",
    }
}

fn map_usage(usage: Option<&Value>) -> Value {
    let Some(u) = usage else {
        return json!({
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        });
    };
    let prompt = u
        .get("prompt_tokens")
        .or_else(|| u.get("inputTokens"))
        .or_else(|| u.get("input_tokens"))
        .or_else(|| u.get("used"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion = u
        .get("completion_tokens")
        .or_else(|| u.get("outputTokens"))
        .or_else(|| u.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total = u
        .get("total_tokens")
        .or_else(|| u.get("totalTokens"))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| prompt.saturating_add(completion));
    json!({
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
    })
}

fn parse_data_uri_image(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let mut parts = meta.split(';');
    let mime = parts.next()?.trim();
    if mime.is_empty() || !mime.starts_with("image/") {
        return None;
    }
    if !parts.any(|p| p.trim().eq_ignore_ascii_case("base64")) {
        return None;
    }
    let cleaned: String = data.chars().filter(|c| !c.is_whitespace()).collect();
    if cleaned.is_empty() {
        return None;
    }
    Some((mime.to_string(), cleaned))
}

fn build_prompt_blocks(messages: &[ChatMessage]) -> Vec<Value> {
    let mut text_buf = String::from(STATELESS_COMPLETION_PREAMBLE);
    let mut image_blocks: Vec<Value> = Vec::new();

    for msg in messages {
        let tag = match msg.role.as_str() {
            "system" | "developer" => "system",
            "assistant" => "assistant",
            "tool" => "tool",
            _ => "user",
        };
        text_buf.push('<');
        text_buf.push_str(tag);
        if let Some(ref name) = msg.name {
            if !name.is_empty() {
                text_buf.push_str(" name=\"");
                text_buf.push_str(name);
                text_buf.push('"');
            }
        }
        if let Some(ref tcid) = msg.tool_call_id {
            if !tcid.is_empty() {
                text_buf.push_str(" tool_call_id=\"");
                text_buf.push_str(tcid);
                text_buf.push('"');
            }
        }
        text_buf.push_str(">\n");

        if let Some(ref content) = msg.content {
            match content {
                MessageContent::Text(s) => {
                    text_buf.push_str(s);
                }
                MessageContent::Parts(parts) => {
                    for part in parts {
                        match part.kind.as_str() {
                            "text" | "input_text" => {
                                if let Some(ref t) = part.text {
                                    text_buf.push_str(t);
                                }
                            }
                            "image_url" | "input_image" => {
                                if let Some(ref img) = part.image_url {
                                    let url = img.as_str();
                                    if let Some((mime, data)) = parse_data_uri_image(url) {
                                        image_blocks.push(json!({
                                            "type": "image",
                                            "mimeType": mime,
                                            "data": data,
                                        }));
                                        text_buf.push_str("\n[Attached image]\n");
                                    } else {
                                        text_buf.push_str("\n[Image URL: ");
                                        text_buf.push_str(url);
                                        text_buf.push_str("]\n");
                                    }
                                }
                            }
                            _ => {
                                if let Some(ref t) = part.text {
                                    text_buf.push_str(t);
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(ref tool_calls) = msg.tool_calls {
            if !tool_calls.is_null() {
                text_buf.push_str("\n<tool_calls>");
                text_buf.push_str(&tool_calls.to_string());
                text_buf.push_str("</tool_calls>");
            }
        }

        text_buf.push_str("\n</");
        text_buf.push_str(tag);
        text_buf.push_str(">\n\n");
    }

    let mut blocks = Vec::with_capacity(1 + image_blocks.len());
    blocks.push(json!({
        "type": "text",
        "text": text_buf.trim_end(),
    }));
    blocks.extend(image_blocks);
    blocks
}

fn format_sse_data(payload: &Value) -> String {
    format!(
        "data: {}\n\n",
        serde_json::to_string(payload).unwrap_or_else(|_| "{}".into())
    )
}

pub(crate) async fn list_models(State(state): State<AppState>) -> Response {
    match state.manager.list_models().await {
        Ok(models) => {
            let data: Vec<Value> = models
                .into_iter()
                .map(|m| {
                    json!({
                        "id": m.id,
                        "object": "model",
                        "created": 0,
                        "owned_by": "acp-gateway",
                    })
                })
                .collect();
            Json(json!({
                "object": "list",
                "data": data,
            }))
            .into_response()
        }
        Err(err) => map_openai_error(err),
    }
}

pub(crate) async fn chat_completions(
    State(state): State<AppState>,
    Json(req): Json<ChatCompletionRequest>,
) -> Response {
    if req.messages.is_empty() {
        return openai_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "messages must not be empty",
        );
    }

    let prompt_blocks = build_prompt_blocks(&req.messages);
    let requested_model = req
        .model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from);
    let requested_effort = req
        .reasoning_effort
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from);
    let include_usage = req
        .stream_options
        .as_ref()
        .is_some_and(|o| o.include_usage);

    let completion_id = format!("chatcmpl-{}", Uuid::new_v4().simple());
    let created = unix_timestamp();

    if !req.stream {
        match state
            .manager
            .complete_stateless(
                prompt_blocks,
                requested_model.as_deref(),
                requested_effort.as_deref(),
                None,
                None,
            )
            .await
        {
            Ok(res) => {
                let mut message = json!({
                    "role": "assistant",
                    "content": res.content,
                });
                if !res.reasoning.is_empty() {
                    message["reasoning_content"] = json!(res.reasoning);
                }
                let model_name = requested_model.unwrap_or(res.model);
                Json(json!({
                    "id": completion_id,
                    "object": "chat.completion",
                    "created": created,
                    "model": model_name,
                    "choices": [{
                        "index": 0,
                        "message": message,
                        "finish_reason": map_finish_reason(&res.stop_reason),
                    }],
                    "usage": map_usage(res.usage.as_ref()),
                }))
                .into_response()
            }
            Err(err) => map_openai_error(err),
        }
    } else {
        let manager = state.manager.clone();
        let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel::<CompletionStreamEvent>();
        let (done_tx, mut done_rx) =
            tokio::sync::oneshot::channel::<Result<crate::runner::StatelessCompletionResult, GatewayError>>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<String>();

        let model_for_worker = requested_model.clone();
        let effort_for_worker = requested_effort.clone();
        let worker = tokio::spawn(async move {
            let res = manager
                .complete_stateless(
                    prompt_blocks,
                    model_for_worker.as_deref(),
                    effort_for_worker.as_deref(),
                    None,
                    Some(chunk_tx),
                )
                .await;
            let _ = done_tx.send(res);
        });

        let fallback_model = requested_model.unwrap_or_else(|| "acp-gateway".into());
        tokio::spawn(async move {
            let mut sent_role = false;
            let mut active_model = fallback_model;

            loop {
                tokio::select! {
                    _ = out_tx.closed() => {
                        worker.abort();
                        return;
                    }
                    recv = tokio::time::timeout(Duration::from_secs(SSE_KEEPALIVE_SECONDS), chunk_rx.recv()) => {
                        match recv {
                            Ok(Some(ev)) => {
                                let mut delta = serde_json::Map::new();
                                if !sent_role {
                                    delta.insert("role".into(), json!("assistant"));
                                    sent_role = true;
                                }
                                match ev {
                                    CompletionStreamEvent::Content(text) => {
                                        delta.insert("content".into(), json!(text));
                                    }
                                    CompletionStreamEvent::Reasoning(text) => {
                                        delta.insert("reasoning_content".into(), json!(text));
                                    }
                                }
                                let chunk = json!({
                                    "id": completion_id,
                                    "object": "chat.completion.chunk",
                                    "created": created,
                                    "model": active_model,
                                    "choices": [{
                                        "index": 0,
                                        "delta": Value::Object(delta),
                                        "finish_reason": Value::Null,
                                    }],
                                });
                                if out_tx.send(format_sse_data(&chunk)).is_err() {
                                    worker.abort();
                                    return;
                                }
                            }
                            Ok(None) => break,
                            Err(_) => {
                                if out_tx.send(": keepalive\n\n".into()).is_err() {
                                    worker.abort();
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            // Wait for worker completion result after chunk_tx closes.
            let final_res = tokio::select! {
                _ = out_tx.closed() => {
                    worker.abort();
                    return;
                }
                res = &mut done_rx => res,
            };

            match final_res {
                Ok(Ok(res)) => {
                    if !res.model.is_empty() && active_model == "acp-gateway" {
                        active_model = res.model;
                    }
                    if !sent_role {
                        let role_chunk = json!({
                            "id": completion_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": active_model,
                            "choices": [{
                                "index": 0,
                                "delta": { "role": "assistant", "content": "" },
                                "finish_reason": Value::Null,
                            }],
                        });
                        if out_tx.send(format_sse_data(&role_chunk)).is_err() {
                            return;
                        }
                    }
                    let finish_chunk = json!({
                        "id": completion_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": active_model,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": map_finish_reason(&res.stop_reason),
                        }],
                    });
                    if out_tx.send(format_sse_data(&finish_chunk)).is_err() {
                        return;
                    }
                    if include_usage {
                        let usage_chunk = json!({
                            "id": completion_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": active_model,
                            "choices": [],
                            "usage": map_usage(res.usage.as_ref()),
                        });
                        if out_tx.send(format_sse_data(&usage_chunk)).is_err() {
                            return;
                        }
                    }
                    let _ = out_tx.send("data: [DONE]\n\n".into());
                }
                Ok(Err(err)) => {
                    let err_payload = json!({
                        "error": {
                            "message": err.to_string(),
                            "type": "server_error",
                            "param": Value::Null,
                            "code": Value::Null,
                        }
                    });
                    let _ = out_tx.send(format_sse_data(&err_payload));
                    let _ = out_tx.send("data: [DONE]\n\n".into());
                }
                Err(_) => {
                    let _ = out_tx.send("data: [DONE]\n\n".into());
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
}
