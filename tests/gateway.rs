use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use acp_gateway::config::Settings;
use acp_gateway::http::create_app;
use acp_gateway::models::StartSessionRequest;
use acp_gateway::runner::{GatewayError, SessionManager};
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tower::ServiceExt;

static ENV_LOCK: Mutex<()> = Mutex::const_new(());

struct FakeEnv {
    _tmp: TempDir,
    settings: Settings,
    folder_a: PathBuf,
    folder_b: PathBuf,
    state_dir: PathBuf,
    root: PathBuf,
}

fn setup_fake_env() -> FakeEnv {
    let tmp = TempDir::new().expect("tempdir");
    let state_dir = tmp.path().join("acp_state");
    fs::create_dir_all(&state_dir).unwrap();

    let fake_bin = tmp.path().join("fake_acp_agent");
    let script = format!(
        r#"#!/usr/bin/env python3
import json
import os
import select
import sys
import uuid

args = sys.argv[1:]
if args and args[0] == "models":
    if "ACP_GATEWAY_API_TOKEN" in os.environ:
        sys.stderr.write("leaked api token\n")
        sys.exit(9)
    sys.stdout.write("\r⠋ Fetching available models...\rgemini-4-neon  Gemini 4 Neon\n")
    sys.stdout.write("gemini-3.8-flash  Gemini 3.8 Flash\n")
    sys.exit(0)

if not args or args[0] != "acp":
    sys.stderr.write(f"unexpected args: {{args}}\n")
    sys.exit(2)

autonomous = "--dangerously-skip-permissions" in args
sandbox = "--sandbox" in args
no_resume = "--no-resume" in args
extra_flags = [a for a in args[1:] if a not in ("--dangerously-skip-permissions", "--sandbox", "--no-resume")]
state_dir = {state_dir:?}
sessions = {{}}
resume_probes = 0
next_server_req_id = 10000

def config_options_for(st):
    return [
        {{
            "id": "model",
            "name": "Model",
            "category": "model",
            "type": "select",
            "currentValue": st["model"],
            "options": [
                {{"value": "gemini-4-neon", "name": "Gemini 4 Neon"}},
                {{"value": "gemini-3.8-flash", "name": "Gemini 3.8 Flash"}},
            ],
        }},
        {{
            "id": "thought_level",
            "name": "Thought Level",
            "category": "thought_level",
            "type": "select",
            "currentValue": st["effort"],
            "options": [
                {{"value": "low", "name": "Low"}},
                {{"value": "high", "name": "High"}},
            ],
        }},
    ]

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def state_path(sid):
    return os.path.join(state_dir, f"{{sid}}.json")

while True:
    raw = sys.stdin.readline()
    if not raw:
        break
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    req_id = msg.get("id")
    method = msg.get("method")
    params = msg.get("params") or {{}}

    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{"protocolVersion": 1, "agentCapabilities": {{"loadSession": True}}}}}})
    elif method == "session/new":
        sid = str(uuid.uuid4())
        sessions[sid] = {{
            "cwd": params["cwd"],
            "model": "gemini-4-neon",
            "effort": "high",
            "history": [],
            "config_calls": 0,
        }}
        send({{
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {{
                "sessionId": sid,
                "configOptions": config_options_for(sessions[sid]),
            }},
        }})
    elif method == "session/resume":
        resume_probes += 1
        if no_resume:
            send({{"jsonrpc": "2.0", "id": req_id, "error": {{"code": -32601, "message": "method not found"}}}})
            continue
        sid = params["sessionId"]
        p = state_path(sid)
        if not os.path.exists(p):
            send({{"jsonrpc": "2.0", "id": req_id, "error": {{"code": -32000, "message": "session not persisted"}}}})
            continue
        with open(p) as f:
            data = json.load(f)
        sessions[sid] = {{
            "cwd": params["cwd"],
            "model": "gemini-4-neon",
            "effort": "high",
            "history": data["history"],
            "config_calls": 0,
        }}
        send({{
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {{
                "configOptions": config_options_for(sessions[sid]),
            }},
        }})
    elif method == "session/load":
        sid = params["sessionId"]
        p = state_path(sid)
        if not os.path.exists(p):
            send({{"jsonrpc": "2.0", "id": req_id, "error": {{"code": -32000, "message": "session not persisted"}}}})
            continue
        with open(p) as f:
            data = json.load(f)
        sessions[sid] = {{
            "cwd": params["cwd"],
            "model": "gemini-4-neon",
            "effort": "high",
            "history": data["history"],
            "config_calls": 0,
        }}
        send({{
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {{
                "configOptions": config_options_for(sessions[sid]),
            }},
        }})
    elif method == "session/set_config_option":
        sid = params["sessionId"]
        cfg_id = params["configId"]
        sessions[sid]["config_calls"] += 1
        if cfg_id == "model":
            sessions[sid]["model"] = params["value"]
        elif cfg_id == "thought_level":
            sessions[sid]["effort"] = params["value"]
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{"configOptions": config_options_for(sessions[sid])}}}})
    elif method == "session/prompt":
        sid = params["sessionId"]
        st = sessions[sid]
        prompt = "".join(b.get("text", "") for b in params.get("prompt") or [] if b.get("type") == "text")
        image_count = sum(1 for b in params.get("prompt") or [] if b.get("type") == "image")
        if prompt == "CRASH":
            sys.exit(1)
        if prompt == "SLOW":
            rlist, _, _ = select.select([sys.stdin], [], [], 2.0)
            if rlist:
                cancel_raw = sys.stdin.readline().strip()
                if cancel_raw:
                    cmsg = json.loads(cancel_raw)
                    if cmsg.get("method") == "session/cancel":
                        with open(os.path.join(state_dir, "cancelled.log"), "a") as cf:
                            cf.write(cmsg["params"]["sessionId"] + "\n")
                        send({{"jsonrpc": "2.0", "id": req_id, "result": {{"stopReason": "cancelled"}}}})
                        continue
            send({{"jsonrpc": "2.0", "id": req_id, "result": {{"stopReason": "end_turn"}}}})
            continue

        perm_outcome = "none"
        if prompt in ("NEED_PERMISSION", "REJECT_ONLY_PERMISSION"):
            next_server_req_id += 1
            perm_id = next_server_req_id
            options = (
                [{{"optionId": "reject_once", "name": "Reject", "kind": "reject_once"}}]
                if prompt == "REJECT_ONLY_PERMISSION"
                else [
                    {{"optionId": "allow_once", "name": "Allow once", "kind": "allow_once"}},
                    {{"optionId": "reject_once", "name": "Reject", "kind": "reject_once"}},
                ]
            )
            send({{
                "jsonrpc": "2.0",
                "id": perm_id,
                "method": "session/request_permission",
                "params": {{
                    "sessionId": sid,
                    "toolCall": {{"toolCallId": "tc-1", "title": "Run tool"}},
                    "options": options,
                }},
            }})
            perm_resp = json.loads(sys.stdin.readline())
            perm_outcome = perm_resp["result"]["outcome"]["outcome"]
        if prompt == "CHECK_TOKEN_ENV" and "ACP_GATEWAY_API_TOKEN" in os.environ:
            sys.exit(9)

        st["history"].append(prompt)
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": sid,
                "update": {{
                    "sessionUpdate": "tool_call",
                    "toolCallId": "tc-1",
                    "title": "Inspect workspace",
                    "kind": "read",
                }},
            }},
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": sid,
                "update": {{
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "tc-1",
                    "status": "completed",
                    "title": "Inspect workspace",
                }},
            }},
        }})
        if "EMIT_THOUGHT" in prompt:
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": sid,
                    "update": {{"sessionUpdate": "agent_thought_chunk", "content": {{"type": "text", "text": "thinking-step"}}}},
                }},
            }})
        reply = (
            f"pid={{os.getpid()}}|cwd={{st['cwd']}}|auto={{autonomous}}|sandbox={{sandbox}}|"
            f"extra={{','.join(extra_flags)}}|perm={{perm_outcome}}|cfg={{st['config_calls']}}|"
            f"resume_probes={{resume_probes}}|model={{st['model']}}/{{st['effort']}}|turn={{len(st['history'])}}|"
            f"first={{st['history'][0]}}|msg={{prompt}}|images={{image_count}}|"
        )
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": sid,
                "update": {{"sessionUpdate": "agent_message_chunk", "content": {{"type": "text", "text": reply}}}},
            }},
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": sid,
                "update": {{"sessionUpdate": "usage_update", "used": 42, "size": 100000}},
            }},
        }})
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{"stopReason": "end_turn"}}}})
    elif method == "session/close":
        sid = params["sessionId"]
        if sid in sessions:
            with open(state_path(sid), "w") as f:
                json.dump(sessions[sid], f)
            sessions.pop(sid, None)
        send({{"jsonrpc": "2.0", "id": req_id, "result": {{}}}})
    elif method == "session/cancel":
        with open(os.path.join(state_dir, "cancelled.log"), "a") as cf:
            cf.write(params.get("sessionId", "") + "\n")
    elif req_id is not None:
        send({{"jsonrpc": "2.0", "id": req_id, "error": {{"code": -32601, "message": f"unknown method {{method}}"}}}})
"#,
        state_dir = state_dir.to_string_lossy()
    );
    fs::write(&fake_bin, script).unwrap();
    let mut perms = fs::metadata(&fake_bin).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&fake_bin, perms).unwrap();

    let root = tmp.path().join("workspaces");
    let folder_a = root.join("folder_a");
    let folder_b = root.join("folder_b");
    fs::create_dir_all(&folder_a).unwrap();
    fs::create_dir_all(&folder_b).unwrap();

    let mut settings = Settings::default();
    settings.acp_command = vec![fake_bin.to_string_lossy().into(), "acp".into()];
    settings.models_command = vec![fake_bin.to_string_lossy().into(), "models".into()];
    settings.db_path = tmp.path().join("sessions.db");
    settings.allowed_base_dirs = vec![root.clone()];
    settings.idle_ttl_seconds = 60;

    FakeEnv {
        _tmp: tmp,
        settings,
        folder_a,
        folder_b,
        state_dir,
        root,
    }
}

fn parse_mcp_sse_messages(raw_body: &str) -> Vec<Value> {
    raw_body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|json_str| serde_json::from_str::<Value>(json_str).expect("valid json in sse"))
        .collect()
}

async fn send_req(
    app: &Router,
    method: Method,
    uri: &str,
    headers: Vec<(&str, Vec<u8>)>,
    body: Option<Value>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    let mut has_host = false;
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("host") {
            has_host = true;
        }
        builder = builder.header(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_bytes(&v).unwrap(),
        );
    }
    if !has_host {
        builder = builder.header("host", "test");
    }
    let req = if let Some(b) = body {
        builder = builder.header("content-type", "application/json");
        builder.body(Body::from(serde_json::to_string(&b).unwrap())).unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    };

    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).to_string();
    (status, resp_headers, text)
}

#[tokio::test]
async fn test_acp_multiplexing_model_switch_skips_noop_and_cold_resume() {
    let env = setup_fake_env();
    let mgr = SessionManager::new(env.settings.clone()).unwrap();
    mgr.start().await;

    let r1 = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "alpha".into(),
                model: Some("gemini-3.8-flash-low".into()),
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(r1.status, "SUCCESS");
    assert!(
        r1.response.contains(
            "auto=True|sandbox=False|extra=|perm=none|cfg=2|resume_probes=0|model=gemini-3.8-flash/low|turn=1|first=alpha|msg=alpha"
        ),
        "unexpected response: {}",
        r1.response
    );
    assert_eq!(
        r1.usage,
        Some(json!({"sessionUpdate": "usage_update", "used": 42, "size": 100000}))
    );
    let pid_a = r1.response.split('|').next().unwrap().to_string();

    // Session B requesting default model/effort skips redundant set_config_option RPCs (cfg=0)
    let r_b = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_b.to_string_lossy().into(),
                prompt: "beta".into(),
                model: Some("gemini-4-neon".into()),
                effort: Some("high".into()),
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();
    let pid_b = r_b.response.split('|').next().unwrap().to_string();
    assert_eq!(pid_a, pid_b);
    assert!(r_b.response.contains("cfg=0|resume_probes=0|model=gemini-4-neon/high"));
    assert!(r_b.response.contains(&fs::canonicalize(&env.folder_b).unwrap().to_string_lossy().to_string()));

    // Warm follow-up switching model via combined <model>-<effort> ID
    let r2 = mgr
        .continue_session(
            &r1.conversation_id,
            "second",
            Some("gemini-4-neon-high"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(r2.num_turns, Some(2));
    assert!(r2.response.contains("model=gemini-4-neon/high|turn=2|first=alpha|msg=second"));

    // Graceful shutdown flushes via session/close; new manager cold-resumes via session/resume
    mgr.shutdown().await;

    let mgr2 = SessionManager::new(env.settings.clone()).unwrap();
    mgr2.start().await;
    assert!(!mgr2.get_session(&r1.conversation_id).unwrap().warm);
    let r3 = mgr2
        .continue_session(&r1.conversation_id, "third", None, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(r3.num_turns, Some(3));
    assert!(r3.response.contains("model=gemini-4-neon/high|turn=3|first=alpha|msg=third"));
    let pid_c = r3.response.split('|').next().unwrap().to_string();
    assert_ne!(pid_c, pid_a);

    mgr2.shutdown().await;
}

#[tokio::test]
async fn test_agent_agnostic_command_and_permission_handling() {
    let env = setup_fake_env();
    let mut custom = env.settings.clone();
    custom.acp_command.push("--custom-agent-flag".into());
    custom.autonomous_flags.clear();
    custom.sandbox_flags = vec!["--custom-sandbox".into()];
    custom.models_command.clear();

    let mgr = SessionManager::new(custom).unwrap();
    mgr.start().await;

    assert!(mgr.list_models().await.unwrap().is_empty());

    let r_auto = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "NEED_PERMISSION".into(),
                model: None,
                effort: None,
                autonomous: Some(true),
                sandbox: Some(true),
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        r_auto
            .response
            .contains("auto=False|sandbox=False|extra=--custom-agent-flag,--custom-sandbox|perm=selected|"),
        "unexpected: {}",
        r_auto.response
    );

    let model_ids: Vec<String> = mgr
        .list_models()
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(model_ids, vec!["gemini-4-neon", "gemini-3.8-flash"]);

    let r_manual = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "NEED_PERMISSION".into(),
                model: None,
                effort: None,
                autonomous: Some(false),
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(r_manual.response.contains("perm=cancelled|"));

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_single_server_shares_warm_pool_across_mcp_streamable_http_and_rest() {
    let env = setup_fake_env();
    let (app, mgr) = create_app(env.settings.clone());
    mgr.start().await;

    let (status, _, _) = send_req(&app, Method::GET, "/health", vec![], None).await;
    assert_eq!(status, StatusCode::OK);

    // 1. Initialize MCP Streamable HTTP session on /mcp
    let (status, headers, _) = send_req(
        &app,
        Method::POST,
        "/mcp",
        vec![("accept", b"application/json, text/event-stream".to_vec())],
        Some(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "rust-test", "version": "1.0"}
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mcp_sid = headers
        .get("mcp-session-id")
        .expect("mcp-session-id header")
        .to_str()
        .unwrap()
        .to_string();

    let (status, _, _) = send_req(
        &app,
        Method::POST,
        "/mcp",
        vec![
            ("accept", b"application/json, text/event-stream".to_vec()),
            ("mcp-session-id", mcp_sid.as_bytes().to_vec()),
        ],
        Some(json!({"jsonrpc": "2.0", "method": "notifications/initialized"})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // 2. Call acp_start via MCP with progressToken -> verify ACP updates forwarded as MCP progress!
    let (status, _, body) = send_req(
        &app,
        Method::POST,
        "/mcp",
        vec![
            ("accept", b"application/json, text/event-stream".to_vec()),
            ("mcp-session-id", mcp_sid.as_bytes().to_vec()),
        ],
        Some(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "acp_start",
                "arguments": {
                    "folder": env.folder_a.to_string_lossy(),
                    "prompt": "hello unified",
                    "model": "gemini-3.8-flash-low"
                },
                "_meta": {"progressToken": "tok-1"}
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events = parse_mcp_sse_messages(&body);
    let progress_notes: Vec<&Value> = events
        .iter()
        .filter(|e| e.get("method").and_then(Value::as_str) == Some("notifications/progress"))
        .collect();
    assert!(progress_notes.iter().any(|n| {
        n.pointer("/params/message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("tool_call: Inspect workspace")
    }));
    assert!(progress_notes.iter().any(|n| {
        n.pointer("/params/message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("tool_completed: Inspect workspace")
    }));

    let tool_reply = events
        .iter()
        .find(|e| e.get("id").and_then(Value::as_i64) == Some(2))
        .expect("tool reply");
    let start_payload = &tool_reply["result"]["structuredContent"];
    let conv_id = start_payload["conversation_id"].as_str().unwrap().to_string();
    let mcp_pid = start_payload["response"]
        .as_str()
        .unwrap()
        .split('|')
        .next()
        .unwrap()
        .to_string();

    // 3. Continue the EXACT SAME warm session over REST + SSE (same PID)
    let (status, _, sse_body) = send_req(
        &app,
        Method::POST,
        &format!("/sessions/{conv_id}/messages"),
        vec![],
        Some(json!({"prompt": "stream me", "stream": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(sse_body.contains("event: agent_message_chunk"));
    assert!(sse_body.contains("event: result"));
    assert!(sse_body.contains(&format!("{mcp_pid}|")));
    assert!(sse_body.contains("msg=stream me"));

    // 4. Models endpoint reads cached ACP models directly
    let (status, _, models_body) = send_req(&app, Method::GET, "/models", vec![], None).await;
    assert_eq!(status, StatusCode::OK);
    let models_json: Vec<Value> = serde_json::from_str(&models_body).unwrap();
    let ids: Vec<&str> = models_json
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["gemini-4-neon", "gemini-3.8-flash"]);

    let (status, _, cancel_body) = send_req(
        &app,
        Method::POST,
        &format!("/sessions/{conv_id}/cancel"),
        vec![],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cancel_json: Value = serde_json::from_str(&cancel_body).unwrap();
    assert_eq!(cancel_json["stopped"], Value::Bool(true));

    let (status, _, sessions_body) = send_req(&app, Method::GET, "/sessions", vec![], None).await;
    assert_eq!(status, StatusCode::OK);
    let sessions_json: Vec<Value> = serde_json::from_str(&sessions_body).unwrap();
    assert_eq!(sessions_json.len(), 1);
    assert_eq!(sessions_json[0]["num_turns"], json!(2));

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_folder_allowlist_and_unknown_session() {
    let env = setup_fake_env();
    let outside = env._tmp.path().join("outside");
    fs::create_dir_all(&outside).unwrap();

    let mgr = SessionManager::new(env.settings.clone()).unwrap();
    let err = mgr
        .start_session(
            StartSessionRequest {
                folder: outside.to_string_lossy().into(),
                prompt: "nope".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, GatewayError::FolderAccess(_)));

    let err = mgr
        .continue_session("missing-id", "hello", None, None, None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, GatewayError::SessionNotFound(_)));

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_turns_and_daemon_crash_recovery() {
    let env = setup_fake_env();
    let mgr = SessionManager::new(env.settings.clone()).unwrap();
    mgr.start().await;

    let r1 = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "t1".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

    let (r2, r3) = tokio::join!(
        mgr.continue_session(&r1.conversation_id, "t2", None, None, None, None, None),
        mgr.continue_session(&r1.conversation_id, "t3", None, None, None, None, None),
    );
    let mut turns = vec![r2.unwrap().num_turns.unwrap(), r3.unwrap().num_turns.unwrap()];
    turns.sort();
    assert_eq!(turns, vec![2, 3]);
    assert_eq!(mgr.get_session(&r1.conversation_id).unwrap().num_turns, 3);

    // Daemon crash fails active call and restarts cleanly on next session
    let crash_err = mgr
        .continue_session(&r1.conversation_id, "CRASH", None, None, None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(crash_err, GatewayError::Gateway { .. }));
    assert_eq!(mgr.get_session(&r1.conversation_id).unwrap().status, "error");

    let r_new = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "recovered".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(r_new.status, "SUCCESS");
    assert!(r_new.response.contains("msg=recovered"));

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_timeout_and_cancellation_notify_acp() {
    let mut env = setup_fake_env();
    env.settings.default_timeout_seconds = 1;
    let mgr = SessionManager::new(env.settings.clone()).unwrap();
    mgr.start().await;

    let err = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "SLOW".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: Some(1),
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, GatewayError::TurnTimeout(_)));
    let sessions = mgr.list_sessions();
    assert_eq!(sessions[0].status, "error");

    let r1 = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "init".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: Some(0),
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

    let mgr_clone = mgr.clone();
    let cid = r1.conversation_id.clone();
    let handle = tokio::spawn(async move {
        let _ = mgr_clone
            .continue_session(&cid, "SLOW", None, None, Some(0), None, None)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.abort();
    let _ = handle.await;

    // Wait briefly for the fake agent to write cancelled.log
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(mgr.get_session(&r1.conversation_id).unwrap().status, "cancelled");
    let cancel_log = fs::read_to_string(env.state_dir.join("cancelled.log")).unwrap_or_default();
    assert!(
        cancel_log.contains(&r1.conversation_id),
        "cancel_log={cancel_log:?}"
    );

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_env_parsing_and_security_boundaries_across_rest_and_mcp() {
    let env = setup_fake_env();

    let mut vars = HashMap::new();
    vars.insert("ACP_GATEWAY_ACP_COMMAND".into(), "opencode acp --foo".into());
    vars.insert("ACP_GATEWAY_AUTONOMOUS_FLAGS".into(), "".into());
    vars.insert(
        "ACP_GATEWAY_ALLOWED_BASE_DIRS".into(),
        format!("{},{}", env.folder_a.display(), env.folder_b.display()),
    );
    let parsed = Settings::from_map(&vars).unwrap();
    assert_eq!(parsed.acp_command, vec!["opencode", "acp", "--foo"]);
    assert!(parsed.autonomous_flags.is_empty());
    assert_eq!(parsed.allowed_base_dirs, vec![env.folder_a.clone(), env.folder_b.clone()]);

    let mut secured = env.settings.clone();
    secured.api_token = Some("secret-tok".into());
    let (app, mgr) = create_app(secured);
    mgr.start().await;

    assert_eq!(send_req(&app, Method::GET, "/health", vec![], None).await.0, StatusCode::OK);
    assert_eq!(send_req(&app, Method::GET, "/sessions", vec![], None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(send_req(&app, Method::POST, "/mcp", vec![], Some(json!({}))).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        send_req(
            &app,
            Method::GET,
            "/sessions",
            vec![("authorization", b"Bearer secret-tok".to_vec())],
            None,
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        send_req(
            &app,
            Method::GET,
            "/sessions",
            vec![
                ("authorization", b"Bearer secret-tok".to_vec()),
                ("host", b"evil.example".to_vec()),
            ],
            None,
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_req(
            &app,
            Method::GET,
            "/sessions",
            vec![
                ("authorization", b"Bearer secret-tok".to_vec()),
                ("origin", b"https://evil.example".to_vec()),
            ],
            None,
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_req(
            &app,
            Method::GET,
            "/sessions",
            vec![
                ("authorization", b"Bearer secret-tok".to_vec()),
                ("origin", b"file://localhost".to_vec()),
            ],
            None,
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_req(
            &app,
            Method::GET,
            "/sessions",
            vec![
                ("authorization", b"Bearer secret-tok".to_vec()),
                ("origin", b"http://localhost:3000".to_vec()),
            ],
            None,
        )
        .await
        .0,
        StatusCode::OK
    );
    // Non-UTF-8 authorization header -> 401
    assert_eq!(
        send_req(
            &app,
            Method::GET,
            "/sessions",
            vec![("authorization", b"Bearer caf\xe9".to_vec())],
            None,
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_hardening_validations_timeouts_env_isolation_and_mcp_errors() {
    let env = setup_fake_env();

    // Config validators reject invalid mcp_path / port / ttl
    assert!(Settings::from_map(&HashMap::from([("ACP_GATEWAY_MCP_PATH".into(), "mcp".into())])).is_err());
    assert!(Settings::from_map(&HashMap::from([("ACP_GATEWAY_MCP_PATH".into(), "/sessions/custom".into())])).is_err());
    assert!(Settings::from_map(&HashMap::from([("ACP_GATEWAY_PORT".into(), "0".into())])).is_err());
    assert!(Settings::from_map(&HashMap::from([("ACP_GATEWAY_IDLE_TTL_SECONDS".into(), "-1".into())])).is_err());

    // Secret API token stripped from child processes (hold ENV_LOCK while mutating process env)
    {
        let _env_guard = ENV_LOCK.lock().await;
        // SAFETY: serialized via ENV_LOCK in test suite
        unsafe { std::env::set_var("ACP_GATEWAY_API_TOKEN", "top-secret-token") };

        let mgr = SessionManager::new(env.settings.clone()).unwrap();
        mgr.store
            .create(
                "stale-1",
                &env.folder_a.to_string_lossy(),
                None,
                None,
                true,
                false,
                "running",
            )
            .unwrap();
        mgr.start().await;

        let stale = mgr.get_session("stale-1").unwrap();
        assert_eq!(stale.status, "error");
        assert_eq!(stale.last_error.as_deref(), Some("Interrupted by server restart"));

        assert!(matches!(
            mgr.validate_folder("relative/path"),
            Err(GatewayError::FolderAccess(_))
        ));

        let models = mgr.list_models().await.unwrap();
        let model_ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(model_ids, vec!["gemini-4-neon", "gemini-3.8-flash"]);

        let r_env = mgr
            .start_session(
                StartSessionRequest {
                    folder: env.folder_a.to_string_lossy().into(),
                    prompt: "CHECK_TOKEN_ENV".into(),
                    model: None,
                    effort: None,
                    autonomous: None,
                    sandbox: None,
                    timeout_seconds: None,
                    stream: false,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(r_env.status, "SUCCESS");

        let r_deny = mgr
            .start_session(
                StartSessionRequest {
                    folder: env.folder_a.to_string_lossy().into(),
                    prompt: "REJECT_ONLY_PERMISSION".into(),
                    model: None,
                    effort: None,
                    autonomous: Some(true),
                    sandbox: None,
                    timeout_seconds: None,
                    stream: false,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert!(r_deny.response.contains("perm=cancelled|"));

        mgr.shutdown().await;
        // SAFETY: serialized via ENV_LOCK in test suite
        unsafe { std::env::remove_var("ACP_GATEWAY_API_TOKEN") };
    }

    // HTTP timeout returns 504; MCP defaults omitted folder and surfaces ToolError cleanly
    let (app, mgr) = create_app(env.settings.clone());
    mgr.start().await;

    let (status, _, _) = send_req(
        &app,
        Method::POST,
        "/sessions",
        vec![],
        Some(json!({"folder": env.folder_a.to_string_lossy(), "prompt": "SLOW", "timeout_seconds": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);

    let (_, headers, _) = send_req(
        &app,
        Method::POST,
        "/mcp",
        vec![("accept", b"application/json, text/event-stream".to_vec())],
        Some(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "rust-test", "version": "1.0"}
            }
        })),
    )
    .await;
    let sid = headers
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let (_, _, default_body) = send_req(
        &app,
        Method::POST,
        "/mcp",
        vec![
            ("accept", b"application/json, text/event-stream".to_vec()),
            ("mcp-session-id", sid.as_bytes().to_vec()),
        ],
        Some(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "acp_start", "arguments": {"prompt": "default-dir"}}
        })),
    )
    .await;
    let events = parse_mcp_sse_messages(&default_body);
    let reply = events.iter().find(|e| e.get("id") == Some(&json!(2))).unwrap();
    assert_ne!(reply["result"]["isError"], json!(true));
    assert!(
        reply["result"]["structuredContent"]["response"]
            .as_str()
            .unwrap()
            .contains(&fs::canonicalize(&env.root).unwrap().to_string_lossy().to_string())
    );

    let (_, _, err_body) = send_req(
        &app,
        Method::POST,
        "/mcp",
        vec![
            ("accept", b"application/json, text/event-stream".to_vec()),
            ("mcp-session-id", sid.as_bytes().to_vec()),
        ],
        Some(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "acp_get_session", "arguments": {"conversation_id": "missing"}}
        })),
    )
    .await;
    let err_events = parse_mcp_sse_messages(&err_body);
    let err_reply = err_events.iter().find(|e| e.get("id") == Some(&json!(3))).unwrap();
    assert_eq!(err_reply["result"]["isError"], json!(true));

    mgr.shutdown().await;
}

#[tokio::test]
async fn test_prewarm_resume_capability_cache_and_persistent_store() {
    let env = setup_fake_env();
    let mut load_only = env.settings.clone();
    load_only.acp_command.push("--no-resume".into());
    load_only.prewarm_daemon = true;
    load_only.daemon_idle_ttl_seconds = 0;

    let mgr = SessionManager::new(load_only).unwrap();
    mgr.start().await;
    mgr.wait_prewarm().await;

    let daemon = mgr.daemon_for(true, false).expect("prewarmed daemon");
    assert!(daemon.is_alive());

    let r1 = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_a.to_string_lossy().into(),
                prompt: "s1".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();
    let r2 = mgr
        .start_session(
            StartSessionRequest {
                folder: env.folder_b.to_string_lossy().into(),
                prompt: "s2".into(),
                model: None,
                effort: None,
                autonomous: None,
                sandbox: None,
                timeout_seconds: None,
                stream: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

    tokio::join!(
        daemon.close_session(&r1.conversation_id),
        daemon.close_session(&r2.conversation_id)
    );

    let c1 = mgr
        .continue_session(&r1.conversation_id, "c1", None, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(daemon.supports_resume(), Some(false));
    assert!(c1.response.contains("resume_probes=1|"));

    let c2 = mgr
        .continue_session(&r2.conversation_id, "c2", None, None, None, None, None)
        .await
        .unwrap();
    assert!(c2.response.contains("resume_probes=1|"));

    mgr.shutdown().await;
    // Idempotent close + transparent reopen on read
    assert!(mgr.store.get(&r1.conversation_id).unwrap().is_some());
    mgr.store.close();
}

#[tokio::test]
async fn test_openai_compatible_models_and_chat_completions() {
    let env = setup_fake_env();

    // /v1 is reserved for OpenAI-compatible routes
    assert!(
        Settings::from_map(&HashMap::from([("ACP_GATEWAY_MCP_PATH".into(), "/v1/mcp".into())]))
            .is_err()
    );

    let mut secured = env.settings.clone();
    secured.api_token = Some("gw-secret".into());
    let expected_cwd = secured.resolved_openai_cwd();
    let (app, mgr) = create_app(secured);
    mgr.start().await;

    // Auth required on /v1/models and /v1/chat/completions
    assert_eq!(
        send_req(&app, Method::GET, "/v1/models", vec![], None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send_req(
            &app,
            Method::POST,
            "/v1/chat/completions",
            vec![],
            Some(json!({"model": "gemini-4-neon", "messages": [{"role": "user", "content": "hi"}]})),
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );

    let auth = vec![("authorization", b"Bearer gw-secret".to_vec())];

    // GET /v1/models returns OpenAI list schema
    let (status, _, body) = send_req(&app, Method::GET, "/v1/models", auth.clone(), None).await;
    assert_eq!(status, StatusCode::OK);
    let models_json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(models_json["object"], "list");
    let ids: Vec<&str> = models_json["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["id"].as_str())
        .collect();
    assert!(ids.contains(&"gemini-4-neon"));
    assert!(ids.contains(&"gemini-3.8-flash"));

    // Empty messages rejected with 400 OpenAI error envelope
    let (status, _, body) = send_req(
        &app,
        Method::POST,
        "/v1/chat/completions",
        auth.clone(),
        Some(json!({"model": "gemini-4-neon", "messages": []})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err_json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(err_json["error"]["type"], "invalid_request_error");

    // Non-streaming completion with model+effort suffix + multimodal image_url
    let (status, _, body) = send_req(
        &app,
        Method::POST,
        "/v1/chat/completions",
        auth.clone(),
        Some(json!({
            "model": "gemini-3.8-flash-low",
            "stream": false,
            "messages": [
                {"role": "system", "content": "Output valid JSON only."},
                {"role": "user", "content": [
                    {"type": "text", "text": "Describe image"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}}
                ]}
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    let comp: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(comp["object"], "chat.completion");
    assert_eq!(comp["model"], "gemini-3.8-flash-low");
    assert_eq!(comp["choices"][0]["finish_reason"], "stop");
    let content = comp["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(content.contains("auto=False|"), "must run non-autonomous: {content}");
    assert!(content.contains("model=gemini-3.8-flash/low|"), "content={content}");
    assert!(content.contains("images=1|"), "must forward image block: {content}");
    assert!(
        content.contains(&fs::canonicalize(&expected_cwd).unwrap().to_string_lossy().to_string()),
        "must use isolated openai cwd: {content}"
    );
    assert!(content.contains("Output valid JSON only."));
    assert!(content.contains("Describe image"));
    // Ephemeral OpenAI completions must not pollute /sessions
    assert!(mgr.list_sessions().is_empty());

    // Streaming SSE completion with reasoning_content + usage + [DONE]
    let (status, _, sse_body) = send_req(
        &app,
        Method::POST,
        "/v1/chat/completions",
        auth.clone(),
        Some(json!({
            "model": "gemini-4-neon",
            "reasoning_effort": "low",
            "stream": true,
            "stream_options": {"include_usage": true},
            "messages": [
                {"role": "user", "content": "EMIT_THOUGHT hello"}
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sse_body={sse_body}");
    assert!(sse_body.contains("data: [DONE]"), "sse_body={sse_body}");
    let chunks: Vec<Value> = sse_body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|d| *d != "[DONE]")
        .map(|d| serde_json::from_str::<Value>(d).unwrap())
        .collect();
    assert!(
        chunks.iter().any(|c| c["choices"][0]["delta"]["reasoning_content"] == "thinking-step"),
        "missing reasoning_content delta: {chunks:?}"
    );
    let combined_content: String = chunks
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
        .collect();
    assert!(combined_content.contains("model=gemini-4-neon/low|"), "combined={combined_content}");
    assert!(chunks.iter().any(|c| c["choices"][0]["finish_reason"] == "stop"));
    assert!(chunks.iter().any(|c| c["usage"]["prompt_tokens"] == 42));
    assert!(mgr.list_sessions().is_empty());

    mgr.shutdown().await;
}
