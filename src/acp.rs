use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const PROTOCOL_VERSION: u64 = 1;
const MAX_LINE_BYTES: usize = 10 * 1024 * 1024; // 10 MB per JSON-RPC line
const STDERR_TAIL_LIMIT: usize = 50;

#[derive(Debug, Error)]
pub enum AcpError {
    #[error("ACP RPC error (code {code}): {message}")]
    Rpc { code: i64, message: String },
    #[error("ACP timed out")]
    Timeout,
    #[error("ACP connection failed: {0}")]
    Disconnected(String),
    #[error("Failed to spawn ACP agent: {0}")]
    Spawn(String),
}

type PendingMap = HashMap<u64, oneshot::Sender<Result<Value, AcpError>>>;
type ListenerMap = HashMap<String, mpsc::UnboundedSender<Value>>;

pub struct AcpConnection {
    next_id: AtomicU64,
    alive: Arc<AtomicBool>,
    stdin_tx: Mutex<Option<mpsc::UnboundedSender<String>>>,
    pending: Arc<Mutex<PendingMap>>,
    listeners: Arc<Mutex<ListenerMap>>,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
    child: tokio::sync::Mutex<Option<Child>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl AcpConnection {
    pub fn spawn(
        cmd: &[String],
        autonomous: bool,
        env: &HashMap<String, String>,
    ) -> Result<Arc<Self>, AcpError> {
        let Some((program, args)) = cmd.split_first() else {
            return Err(AcpError::Spawn("empty command".into()));
        };

        let mut command = Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .map_err(|e| AcpError::Spawn(format!("{program}: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AcpError::Spawn("failed to capture child stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AcpError::Spawn("failed to capture child stdout".into()))?;
        let stderr = child.stderr.take();

        let alive = Arc::new(AtomicBool::new(true));
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(HashMap::new()));
        let listeners: Arc<Mutex<ListenerMap>> = Arc::new(Mutex::new(HashMap::new()));
        let stderr_tail: Arc<Mutex<VecDeque<String>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LIMIT)));

        let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();

        let writer_alive = alive.clone();
        let writer_pending = pending.clone();
        let writer_task = tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(mut line) = stdin_rx.recv().await {
                if !line.ends_with('\n') {
                    line.push('\n');
                }
                if let Err(e) = stdin.write_all(line.as_bytes()).await {
                    writer_alive.store(false, Ordering::SeqCst);
                    fail_all_pending(&writer_pending, &format!("stdin write failed: {e}"));
                    break;
                }
                if let Err(e) = stdin.flush().await {
                    writer_alive.store(false, Ordering::SeqCst);
                    fail_all_pending(&writer_pending, &format!("stdin flush failed: {e}"));
                    break;
                }
            }
        });

        let mut tasks = vec![writer_task];

        if let Some(stderr) = stderr {
            let tail = stderr_tail.clone();
            tasks.push(tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut buf = Vec::new();
                loop {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let line = String::from_utf8_lossy(&buf)
                                .trim_end_matches(['\r', '\n'])
                                .to_string();
                            let mut guard = tail.lock().unwrap_or_else(|e| e.into_inner());
                            if guard.len() >= STDERR_TAIL_LIMIT {
                                guard.pop_front();
                            }
                            guard.push_back(line);
                        }
                    }
                }
            }));
        }

        let reader_alive = alive.clone();
        let reader_pending = pending.clone();
        let reader_listeners = listeners.clone();
        let reader_stdin = stdin_tx.clone();
        let reader_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut buf = Vec::new();
            loop {
                buf.clear();
                match read_limited_line(&mut reader, &mut buf, MAX_LINE_BYTES).await {
                    Ok(0) => {
                        reader_alive.store(false, Ordering::SeqCst);
                        fail_all_pending(&reader_pending, "ACP agent closed stdout (EOF)");
                        break;
                    }
                    Err(e) => {
                        reader_alive.store(false, Ordering::SeqCst);
                        fail_all_pending(&reader_pending, &format!("stdout read error: {e}"));
                        break;
                    }
                    Ok(_) => {
                        let trimmed = String::from_utf8_lossy(&buf);
                        let trimmed = trimmed.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let msg: Value = match serde_json::from_str(trimmed) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::debug!("Ignoring non-JSON line from ACP agent: {e}");
                                continue;
                            }
                        };

                        if let Some(method) = msg.get("method").and_then(Value::as_str) {
                            if let Some(req_id) = msg.get("id") {
                                handle_server_request(
                                    &reader_stdin,
                                    autonomous,
                                    req_id.clone(),
                                    method,
                                    msg.get("params").cloned().unwrap_or_else(|| json!({})),
                                );
                            } else if method == "session/update" {
                                if let Some(params) = msg.get("params") {
                                    if let (Some(sid), Some(update)) = (
                                        params.get("sessionId").and_then(Value::as_str),
                                        params.get("update"),
                                    ) {
                                        let tx = {
                                            let guard = reader_listeners
                                                .lock()
                                                .unwrap_or_else(|e| e.into_inner());
                                            guard.get(sid).cloned()
                                        };
                                        if let Some(tx) = tx {
                                            let _ = tx.send(update.clone());
                                        }
                                    }
                                }
                            }
                        } else if let Some(id_val) = msg.get("id").and_then(Value::as_u64) {
                            let tx = {
                                let mut guard =
                                    reader_pending.lock().unwrap_or_else(|e| e.into_inner());
                                guard.remove(&id_val)
                            };
                            if let Some(tx) = tx {
                                if let Some(err) = msg.get("error") {
                                    let code =
                                        err.get("code").and_then(Value::as_i64).unwrap_or(-32000);
                                    let message = err
                                        .get("message")
                                        .and_then(Value::as_str)
                                        .unwrap_or("unknown ACP error")
                                        .to_string();
                                    let _ = tx.send(Err(AcpError::Rpc { code, message }));
                                } else {
                                    let res = msg.get("result").cloned().unwrap_or(Value::Null);
                                    let _ = tx.send(Ok(res));
                                }
                            }
                        }
                    }
                }
            }
        });
        tasks.push(reader_task);

        Ok(Arc::new(Self {
            next_id: AtomicU64::new(1),
            alive,
            stdin_tx: Mutex::new(Some(stdin_tx)),
            pending,
            listeners,
            stderr_tail,
            child: tokio::sync::Mutex::new(Some(child)),
            tasks: Mutex::new(tasks),
        }))
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub fn stderr_text(&self) -> String {
        let guard = self.stderr_tail.lock().unwrap_or_else(|e| e.into_inner());
        guard.iter().cloned().collect::<Vec<_>>().join("\n").trim().to_string()
    }

    pub fn register_listener(&self, session_id: &str, tx: mpsc::UnboundedSender<Value>) {
        let mut guard = self.listeners.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(session_id.to_string(), tx);
    }

    pub fn unregister_listener(&self, session_id: &str) {
        let mut guard = self.listeners.lock().unwrap_or_else(|e| e.into_inner());
        guard.remove(session_id);
    }

    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value, AcpError> {
        if !self.is_alive() {
            return Err(AcpError::Disconnected(self.stderr_text()));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            guard.insert(id, tx);
        }

        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&payload)
            .map_err(|e| AcpError::Disconnected(format!("serialize error: {e}")))?;

        let send_res = {
            let guard = self.stdin_tx.lock().unwrap_or_else(|e| e.into_inner());
            guard.as_ref().map(|s| s.send(line))
        };
        if !matches!(send_res, Some(Ok(()))) {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            guard.remove(&id);
            return Err(AcpError::Disconnected("stdin channel closed".into()));
        }

        let recv = match timeout {
            Some(dur) if !dur.is_zero() => match tokio::time::timeout(dur, rx).await {
                Ok(r) => r,
                Err(_) => {
                    let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
                    guard.remove(&id);
                    return Err(AcpError::Timeout);
                }
            },
            _ => rx.await,
        };

        match recv {
            Ok(res) => res,
            Err(_) => Err(AcpError::Disconnected("response channel dropped".into())),
        }
    }

    pub fn notify(&self, method: &str, params: Value) -> Result<(), AcpError> {
        if !self.is_alive() {
            return Err(AcpError::Disconnected(self.stderr_text()));
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&payload)
            .map_err(|e| AcpError::Disconnected(format!("serialize error: {e}")))?;
        let guard = self.stdin_tx.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(s) if s.send(line).is_ok() => Ok(()),
            _ => Err(AcpError::Disconnected("stdin channel closed".into())),
        }
    }

    pub async fn shutdown(&self) {
        self.alive.store(false, Ordering::SeqCst);
        {
            let mut guard = self.stdin_tx.lock().unwrap_or_else(|e| e.into_inner());
            *guard = None;
        }
        fail_all_pending(&self.pending, "ACP daemon shutting down");

        let mut child_guard = self.child.lock().await;
        if let Some(mut child) = child_guard.take() {
            if tokio::time::timeout(Duration::from_secs(2), child.wait())
                .await
                .is_err()
            {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
        }
        drop(child_guard);

        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *guard)
        };
        for h in handles {
            h.abort();
        }
    }
}

fn handle_server_request(
    stdin_tx: &mpsc::UnboundedSender<String>,
    autonomous: bool,
    req_id: Value,
    method: &str,
    params: Value,
) {
    let response = if method == "session/request_permission" {
        let outcome = select_permission_outcome(autonomous, &params);
        json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": { "outcome": outcome }
        })
    } else {
        json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": { "code": -32601, "message": format!("method not found: {method}") }
        })
    };

    if let Ok(line) = serde_json::to_string(&response) {
        let _ = stdin_tx.send(line);
    }
}

fn select_permission_outcome(autonomous: bool, params: &Value) -> Value {
    if autonomous {
        if let Some(options) = params.get("options").and_then(Value::as_array) {
            if let Some(allow) = options.iter().find(|o| {
                matches!(
                    o.get("kind").and_then(Value::as_str),
                    Some("allow_once" | "allow_always")
                )
            }) {
                if let Some(option_id) = allow.get("optionId").and_then(Value::as_str) {
                    return json!({
                        "outcome": "selected",
                        "optionId": option_id
                    });
                }
            }
            tracing::warn!(
                "ACP request_permission offered no allow option; denying"
            );
        }
    }
    json!({ "outcome": "cancelled" })
}

fn fail_all_pending(pending: &Arc<Mutex<PendingMap>>, msg: &str) {
    let drained: Vec<_> = {
        let mut guard = pending.lock().unwrap_or_else(|e| e.into_inner());
        guard.drain().collect()
    };
    for (_, tx) in drained {
        let _ = tx.send(Err(AcpError::Disconnected(msg.to_string())));
    }
}

async fn read_limited_line<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<usize> {
    let mut total = 0;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(total);
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if total + take > limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "JSON-RPC line exceeded 10 MB limit",
                ));
            }
            buf.extend_from_slice(&available[..take]);
            reader.consume(take);
            total += take;
            return Ok(total);
        } else {
            let len = available.len();
            if total + len > limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "JSON-RPC line exceeded 10 MB limit",
                ));
            }
            buf.extend_from_slice(available);
            reader.consume(len);
            total += len;
        }
    }
}
