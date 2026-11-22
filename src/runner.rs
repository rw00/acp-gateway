use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use regex::Regex;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::VERSION;
use crate::acp::{AcpConnection, AcpError, PROTOCOL_VERSION};
use crate::config::{Settings, expand_tilde};
use crate::models::{ModelInfo, SessionInfo, StartSessionRequest, TurnResult};
use crate::store::{SessionStore, SessionUpdate};

const EFFORT_SUFFIXES: &[&str] = &["-xhigh", "-high", "-medium", "-low", "-max"];
const SECRET_ENV_KEYS: &[&str] = &["ACP_GATEWAY_API_TOKEN"];

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("{0}")]
    FolderAccess(String),
    #[error("{0}")]
    SessionNotFound(String),
    #[error("{0}")]
    TurnTimeout(String),
    #[error("{message}")]
    Gateway { message: String, code: Option<i64> },
}

impl GatewayError {
    pub fn gateway(msg: impl Into<String>) -> Self {
        Self::Gateway {
            message: msg.into(),
            code: None,
        }
    }

    pub fn code(&self) -> Option<i64> {
        match self {
            Self::Gateway { code, .. } => *code,
            _ => None,
        }
    }
}

pub type EventSender = mpsc::UnboundedSender<Value>;
pub type UpdateSender = mpsc::UnboundedSender<Value>;

#[derive(Debug, Clone)]
pub enum CompletionStreamEvent {
    Content(String),
    Reasoning(String),
}

#[derive(Debug, Clone)]
pub struct StatelessCompletionResult {
    pub model: String,
    pub content: String,
    pub reasoning: String,
    pub stop_reason: String,
    pub usage: Option<Value>,
}

fn child_env() -> HashMap<String, String> {
    std::env::vars()
        .filter(|(k, _)| !SECRET_ENV_KEYS.contains(&k.as_str()))
        .collect()
}

fn ansi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1b\[[0-9;]*[A-Za-z]").expect("valid regex"))
}

fn model_id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]*$").expect("valid regex"))
}

fn iter_select_options(opt: &Value) -> Vec<&Value> {
    let mut out = Vec::new();
    if let Some(items) = opt.get("options").and_then(Value::as_array) {
        for item in items {
            if let Some(grouped) = item.get("options").and_then(Value::as_array) {
                for g in grouped {
                    out.push(g);
                }
            } else {
                out.push(item);
            }
        }
    }
    out
}

fn find_select_option<'a>(
    config_options: &'a [Value],
    category: &str,
    fallback_id: &str,
) -> Option<&'a Value> {
    config_options.iter().find(|opt| {
        let is_select = opt.get("type").and_then(Value::as_str) == Some("select")
            || opt.get("options").and_then(Value::as_array).is_some();
        if !is_select {
            return false;
        }
        opt.get("category").and_then(Value::as_str) == Some(category)
            || opt.get("id").and_then(Value::as_str) == Some(fallback_id)
    })
}

fn resolve_model_and_effort(
    model: Option<&str>,
    effort: Option<&str>,
    config_options: &[Value],
) -> (Option<String>, Option<String>) {
    let Some(model_str) = model.filter(|m| !m.is_empty()) else {
        return (None, effort.map(String::from));
    };

    let valid_models: HashSet<&str> = find_select_option(config_options, "model", "model")
        .map(|opt| {
            iter_select_options(opt)
                .into_iter()
                .filter_map(|o| o.get("value").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();

    if valid_models.is_empty() || valid_models.contains(model_str) {
        return (Some(model_str.to_string()), effort.map(String::from));
    }

    for suffix in EFFORT_SUFFIXES {
        if let Some(base) = model_str.strip_suffix(suffix) {
            if valid_models.contains(base) {
                let eff = effort
                    .map(String::from)
                    .unwrap_or_else(|| suffix.trim_start_matches('-').to_string());
                return (Some(base.to_string()), Some(eff));
            }
        }
    }

    (Some(model_str.to_string()), effort.map(String::from))
}

struct DaemonState {
    conn: Option<Arc<AcpConnection>>,
    initialized: bool,
    available_models: Vec<ModelInfo>,
    config_options: Vec<Value>,
    session_config_options: HashMap<String, Vec<Value>>,
    active_sessions: HashMap<String, Instant>,
    last_activity: Instant,
    inflight: usize,
    supports_resume: Option<bool>,
}

pub struct AcpDaemon {
    settings: Settings,
    autonomous: bool,
    sandbox: bool,
    extra_flags: Vec<String>,
    start_lock: tokio::sync::Mutex<()>,
    state: Mutex<DaemonState>,
}

struct InflightGuard {
    daemon: Arc<AcpDaemon>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let mut st = self.daemon.state.lock().unwrap_or_else(|e| e.into_inner());
        st.inflight = st.inflight.saturating_sub(1);
        st.last_activity = Instant::now();
    }
}

impl AcpDaemon {
    pub fn new(settings: Settings, autonomous: bool, sandbox: bool) -> Arc<Self> {
        Self::new_with_flags(settings, autonomous, sandbox, Vec::new())
    }

    pub fn new_with_flags(
        settings: Settings,
        autonomous: bool,
        sandbox: bool,
        extra_flags: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            settings,
            autonomous,
            sandbox,
            extra_flags,
            start_lock: tokio::sync::Mutex::new(()),
            state: Mutex::new(DaemonState {
                conn: None,
                initialized: false,
                available_models: Vec::new(),
                config_options: Vec::new(),
                session_config_options: HashMap::new(),
                active_sessions: HashMap::new(),
                last_activity: Instant::now(),
                inflight: 0,
                supports_resume: None,
            }),
        })
    }

    fn track_inflight(self: &Arc<Self>) -> InflightGuard {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.inflight += 1;
        st.last_activity = Instant::now();
        InflightGuard {
            daemon: self.clone(),
        }
    }

    pub fn is_alive(&self) -> bool {
        let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.initialized && st.conn.as_ref().is_some_and(|c| c.is_alive())
    }

    pub fn supports_resume(&self) -> Option<bool> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .supports_resume
    }

    fn set_supports_resume(&self, val: bool) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .supports_resume = Some(val);
    }

    fn conn(&self) -> Result<Arc<AcpConnection>, GatewayError> {
        let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !st.initialized {
            return Err(GatewayError::gateway("ACP daemon is not running"));
        }
        match st.conn.as_ref() {
            Some(c) if c.is_alive() => Ok(c.clone()),
            Some(c) => {
                let stderr = c.stderr_text();
                let msg = if stderr.is_empty() {
                    "ACP daemon is not running".to_string()
                } else {
                    format!("ACP daemon is not running: {stderr}")
                };
                Err(GatewayError::gateway(msg))
            }
            None => Err(GatewayError::gateway("ACP daemon is not running")),
        }
    }

    fn build_command(&self) -> Vec<String> {
        let mut cmd = self.settings.acp_command.clone();
        if self.autonomous {
            cmd.extend(self.settings.autonomous_flags.iter().cloned());
        }
        if self.sandbox {
            cmd.extend(self.settings.sandbox_flags.iter().cloned());
        }
        cmd.extend(self.extra_flags.iter().cloned());
        cmd
    }

    pub async fn ensure_started(&self) -> Result<(), GatewayError> {
        if self.is_alive() {
            return Ok(());
        }
        let _guard = self.start_lock.lock().await;
        if self.is_alive() {
            return Ok(());
        }
        self.start_unlocked().await
    }

    async fn start_unlocked(&self) -> Result<(), GatewayError> {
        let old_conn = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.initialized = false;
            st.supports_resume = None;
            st.active_sessions.clear();
            st.session_config_options.clear();
            st.conn.take()
        };
        if let Some(old) = old_conn {
            old.shutdown().await;
        }

        let cmd = self.build_command();
        let env = child_env();
        let conn = AcpConnection::spawn(&cmd, self.autonomous, &env).map_err(|e| {
            GatewayError::gateway(format!("Failed to start/initialize ACP agent: {e}"))
        })?;

        let init_res = conn
            .request(
                "initialize",
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "clientCapabilities": {},
                    "clientInfo": {
                        "name": "acp-gateway",
                        "version": VERSION
                    }
                }),
                Some(Duration::from_secs(30)),
            )
            .await;

        let init_val = match init_res {
            Ok(v) => v,
            Err(AcpError::Timeout) => {
                conn.shutdown().await;
                return Err(GatewayError::TurnTimeout(
                    "ACP initialize timed out after 30s".into(),
                ));
            }
            Err(AcpError::Rpc { code, message }) => {
                conn.shutdown().await;
                return Err(GatewayError::Gateway {
                    message: format!("ACP initialize error: {message}"),
                    code: Some(code),
                });
            }
            Err(e) => {
                let detail = conn.stderr_text();
                conn.shutdown().await;
                let suffix = if detail.is_empty() {
                    String::new()
                } else {
                    format!(" ({detail})")
                };
                return Err(GatewayError::gateway(format!(
                    "Failed to start/initialize ACP agent ({}): {e}{suffix}",
                    cmd.first().map(String::as_str).unwrap_or("")
                )));
            }
        };

        let supports_resume = init_val
            .pointer("/agentCapabilities/sessionCapabilities/resume")
            .is_some_and(|v| !v.is_null());

        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.conn = Some(conn);
        st.initialized = true;
        st.supports_resume = if supports_resume { Some(true) } else { None };
        st.last_activity = Instant::now();
        Ok(())
    }

    async fn call(
        self: &Arc<Self>,
        op: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value, GatewayError> {
        let _inflight = self.track_inflight();
        let conn = self.conn()?;
        match conn.request(op, params, timeout).await {
            Ok(v) => Ok(v),
            Err(AcpError::Timeout) => {
                let secs = timeout.map(|d| d.as_secs_f64()).unwrap_or(0.0);
                Err(GatewayError::TurnTimeout(format!(
                    "ACP {op} timed out after {secs}s"
                )))
            }
            Err(AcpError::Rpc { code, message }) => Err(GatewayError::Gateway {
                message: format!("ACP {op} error: {message}"),
                code: Some(code),
            }),
            Err(e) => {
                let detail = conn.stderr_text();
                {
                    let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    st.initialized = false;
                    st.active_sessions.clear();
                    st.session_config_options.clear();
                }
                let suffix = if detail.is_empty() {
                    String::new()
                } else {
                    format!(" ({detail})")
                };
                tracing::warn!("ACP {op} connection failed: {e}{suffix}");
                Err(GatewayError::gateway(format!(
                    "ACP {op} connection failed: {e}{suffix}"
                )))
            }
        }
    }

    fn record_config_options(&self, session_id: &str, opts: &[Value]) {
        if opts.is_empty() {
            return;
        }
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.config_options = opts.to_vec();
        st.session_config_options
            .insert(session_id.to_string(), opts.to_vec());
        if let Some(model_opt) = find_select_option(opts, "model", "model") {
            let parsed: Vec<ModelInfo> = iter_select_options(model_opt)
                .into_iter()
                .filter_map(|o| {
                    let id = o.get("value").and_then(Value::as_str)?;
                    let label = o.get("name").and_then(Value::as_str).unwrap_or(id);
                    Some(ModelInfo {
                        id: id.to_string(),
                        label: label.to_string(),
                    })
                })
                .collect();
            if !parsed.is_empty() && st.available_models.is_empty() {
                st.available_models = parsed;
            }
        }
    }

    fn record_session_models(&self, session_resp: &Value) {
        let Some(items) = session_resp
            .pointer("/models/availableModels")
            .and_then(Value::as_array)
        else {
            return;
        };
        let parsed: Vec<ModelInfo> = items
            .iter()
            .filter_map(|o| {
                let id = o
                    .get("modelId")
                    .or_else(|| o.get("id"))
                    .and_then(Value::as_str)?;
                let label = o.get("name").and_then(Value::as_str).unwrap_or(id);
                Some(ModelInfo {
                    id: id.to_string(),
                    label: label.to_string(),
                })
            })
            .collect();
        if !parsed.is_empty() {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.available_models = parsed;
        }
    }

    pub async fn close_session(&self, session_id: &str) {
        let conn = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.active_sessions.remove(session_id);
            st.session_config_options.remove(session_id);
            if st.initialized {
                st.conn.clone()
            } else {
                None
            }
        };
        if let Some(conn) = conn {
            if conn.is_alive() {
                if let Err(e) = conn
                    .request(
                        "session/close",
                        json!({ "sessionId": session_id }),
                        Some(Duration::from_secs(10)),
                    )
                    .await
                {
                    tracing::debug!("ACP close_session failed for {session_id}: {e}");
                }
            }
        }
    }

    pub async fn shutdown(&self) {
        let sids: Vec<String> = {
            let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if st.initialized && st.conn.as_ref().is_some_and(|c| c.is_alive()) {
                st.active_sessions.keys().cloned().collect()
            } else {
                Vec::new()
            }
        };
        for sid in sids {
            self.close_session(&sid).await;
        }

        let conn = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.initialized = false;
            st.active_sessions.clear();
            st.session_config_options.clear();
            st.conn.take()
        };
        if let Some(conn) = conn {
            conn.shutdown().await;
        }
    }
}

pub struct SessionManager {
    pub settings: Settings,
    pub store: Arc<SessionStore>,
    daemons: Mutex<HashMap<(bool, bool), Arc<AcpDaemon>>>,
    completions_daemon: Mutex<Option<Arc<AcpDaemon>>>,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    externally_cancelled: Arc<Mutex<HashSet<String>>>,
    reaper_task: Mutex<Option<JoinHandle<()>>>,
    prewarm_task: Mutex<Option<JoinHandle<()>>>,
}

impl SessionManager {
    pub fn new(settings: Settings) -> Result<Arc<Self>, GatewayError> {
        let store = SessionStore::new(settings.db_path.clone())
            .map_err(|e| GatewayError::gateway(format!("Failed to open session store: {e}")))?;
        Ok(Arc::new(Self {
            settings,
            store: Arc::new(store),
            daemons: Mutex::new(HashMap::new()),
            completions_daemon: Mutex::new(None),
            locks: Mutex::new(HashMap::new()),
            externally_cancelled: Arc::new(Mutex::new(HashSet::new())),
            reaper_task: Mutex::new(None),
            prewarm_task: Mutex::new(None),
        }))
    }

    pub async fn start(self: &Arc<Self>) {
        match self.store.reset_stale_running() {
            Ok(n) if n > 0 => {
                tracing::info!("Marked {n} stale running session(s) as interrupted on startup");
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("Failed resetting stale running sessions: {e}"),
        }

        if self.settings.idle_ttl_seconds > 0 || self.settings.daemon_idle_ttl_seconds > 0 {
            let mut guard = self.reaper_task.lock().unwrap_or_else(|e| e.into_inner());
            if guard.is_none() {
                let mgr = self.clone();
                *guard = Some(tokio::spawn(async move {
                    mgr.reaper_loop().await;
                }));
            }
        }

        if self.settings.prewarm_daemon {
            let mut guard = self.prewarm_task.lock().unwrap_or_else(|e| e.into_inner());
            if guard.is_none() {
                let mgr = self.clone();
                *guard = Some(tokio::spawn(async move {
                    let _ = mgr
                        .get_daemon(mgr.settings.default_autonomous, mgr.settings.default_sandbox)
                        .await;
                }));
            }
        }
    }

    pub async fn wait_prewarm(&self) {
        let handle = {
            let mut guard = self.prewarm_task.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(h) = handle {
            let _ = h.await;
        }
    }

    pub fn daemon_for(&self, autonomous: bool, sandbox: bool) -> Option<Arc<AcpDaemon>> {
        self.daemons
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(autonomous, sandbox))
            .cloned()
    }

    pub async fn shutdown(&self) {
        let prewarm = {
            self.prewarm_task
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
        };
        if let Some(h) = prewarm {
            h.abort();
            let _ = h.await;
        }
        let reaper = {
            self.reaper_task
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
        };
        if let Some(h) = reaper {
            h.abort();
            let _ = h.await;
        }

        let mut daemons: Vec<Arc<AcpDaemon>> = {
            let mut guard = self.daemons.lock().unwrap_or_else(|e| e.into_inner());
            guard.drain().map(|(_, d)| d).collect()
        };
        if let Some(cd) = self
            .completions_daemon
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            daemons.push(cd);
        }
        for d in daemons {
            d.shutdown().await;
        }
        self.store.close();
    }

    async fn get_completions_daemon(&self) -> Result<Arc<AcpDaemon>, GatewayError> {
        let daemon = {
            let mut guard = self
                .completions_daemon
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard
                .get_or_insert_with(|| {
                    AcpDaemon::new_with_flags(
                        self.settings.clone(),
                        false,
                        false,
                        self.settings.resolved_openai_acp_flags(),
                    )
                })
                .clone()
        };
        daemon.ensure_started().await?;
        daemon
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_activity = Instant::now();
        Ok(daemon)
    }

    pub async fn complete_stateless(
        &self,
        prompt_blocks: Vec<Value>,
        model: Option<&str>,
        effort: Option<&str>,
        timeout_seconds: Option<u64>,
        on_chunk: Option<mpsc::UnboundedSender<CompletionStreamEvent>>,
    ) -> Result<StatelessCompletionResult, GatewayError> {
        let cwd = self.settings.resolved_openai_cwd();
        std::fs::create_dir_all(&cwd).map_err(|e| {
            GatewayError::gateway(format!(
                "Failed to prepare isolated completion cwd {}: {e}",
                cwd.display()
            ))
        })?;
        let cwd = std::fs::canonicalize(&cwd).unwrap_or(cwd);

        let effective_model = model
            .map(String::from)
            .or_else(|| self.settings.default_model.clone());
        let effective_effort = effort
            .map(String::from)
            .or_else(|| self.settings.default_effort.clone());
        let effective_timeout = timeout_seconds.unwrap_or(self.settings.default_timeout_seconds);

        let daemon = self.get_completions_daemon().await?;
        let _inflight = daemon.track_inflight();

        let session_resp = daemon
            .call(
                "session/new",
                json!({
                    "cwd": cwd.to_string_lossy(),
                    "mcpServers": []
                }),
                Some(Duration::from_secs(30)),
            )
            .await?;

        let session_id = session_resp
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                GatewayError::gateway(format!("ACP session/new missing sessionId: {session_resp}"))
            })?
            .to_string();

        let config_options = session_resp
            .get("configOptions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        daemon.record_config_options(&session_id, &config_options);
        daemon.record_session_models(&session_resp);

        let (resolved_model, _resolved_effort) = match self
            .apply_config(
                &daemon,
                &session_id,
                effective_model.as_deref(),
                effective_effort.as_deref(),
                Some(&config_options),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                daemon.close_session(&session_id).await;
                return Err(e);
            }
        };

        let reported_model = effective_model
            .or(resolved_model)
            .or_else(|| {
                find_select_option(&config_options, "model", "model")
                    .and_then(|o| o.get("currentValue"))
                    .and_then(Value::as_str)
                    .map(String::from)
            })
            .unwrap_or_default();

        let conn = match daemon.conn() {
            Ok(c) => c,
            Err(e) => {
                daemon.close_session(&session_id).await;
                return Err(e);
            }
        };
        let (update_tx, mut update_rx) = mpsc::unbounded_channel::<Value>();
        conn.register_listener(&session_id, update_tx);

        struct EphemeralSessionGuard {
            armed: bool,
            conn: Arc<AcpConnection>,
            daemon: Arc<AcpDaemon>,
            session_id: String,
        }
        impl Drop for EphemeralSessionGuard {
            fn drop(&mut self) {
                self.conn.unregister_listener(&self.session_id);
                if self.armed {
                    let _ = self.conn.notify(
                        "session/cancel",
                        json!({ "sessionId": self.session_id }),
                    );
                    let daemon = self.daemon.clone();
                    let sid = self.session_id.clone();
                    tokio::spawn(async move {
                        daemon.close_session(&sid).await;
                    });
                }
            }
        }

        let mut guard = EphemeralSessionGuard {
            armed: true,
            conn: conn.clone(),
            daemon: daemon.clone(),
            session_id: session_id.clone(),
        };

        let daemon_for_updates = daemon.clone();
        let update_collector = tokio::spawn(async move {
            let mut content_chunks: Vec<String> = Vec::new();
            let mut reasoning_chunks: Vec<String> = Vec::new();
            let mut usage: Option<Value> = None;
            while let Some(update) = update_rx.recv().await {
                {
                    let mut st = daemon_for_updates
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    st.last_activity = Instant::now();
                }

                let kind = update
                    .get("sessionUpdate")
                    .and_then(Value::as_str)
                    .unwrap_or("update");

                if kind == "agent_message_chunk" {
                    if update.pointer("/content/type").and_then(Value::as_str) == Some("text") {
                        if let Some(text) = update.pointer("/content/text").and_then(Value::as_str)
                        {
                            content_chunks.push(text.to_string());
                            if let Some(ref tx) = on_chunk {
                                let _ = tx.send(CompletionStreamEvent::Content(text.to_string()));
                            }
                        }
                    }
                } else if kind == "agent_thought_chunk" {
                    if update.pointer("/content/type").and_then(Value::as_str) == Some("text") {
                        if let Some(text) = update.pointer("/content/text").and_then(Value::as_str)
                        {
                            reasoning_chunks.push(text.to_string());
                            if let Some(ref tx) = on_chunk {
                                let _ = tx.send(CompletionStreamEvent::Reasoning(text.to_string()));
                            }
                        }
                    }
                } else if kind == "usage_update" {
                    usage = Some(update.clone());
                }
            }
            (content_chunks, reasoning_chunks, usage)
        });

        let timeout = if effective_timeout > 0 {
            Some(Duration::from_secs(effective_timeout))
        } else {
            None
        };

        let prompt_res = daemon
            .call(
                "session/prompt",
                json!({
                    "sessionId": session_id,
                    "prompt": prompt_blocks,
                }),
                timeout,
            )
            .await;

        guard.armed = false;
        conn.unregister_listener(&session_id);
        drop(guard);

        let res = match prompt_res {
            Ok(v) => v,
            Err(GatewayError::TurnTimeout(msg)) => {
                let _ = conn.notify("session/cancel", json!({ "sessionId": session_id }));
                update_collector.abort();
                daemon.close_session(&session_id).await;
                return Err(GatewayError::TurnTimeout(msg));
            }
            Err(err) => {
                update_collector.abort();
                daemon.close_session(&session_id).await;
                return Err(err);
            }
        };

        let (content_chunks, reasoning_chunks, mut usage) =
            update_collector.await.unwrap_or_default();
        daemon.close_session(&session_id).await;

        let stop_reason = res
            .get("stopReason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn")
            .to_string();
        if let Some(u) = res.get("usage").filter(|v| !v.is_null()) {
            usage = Some(u.clone());
        }

        Ok(StatelessCompletionResult {
            model: reported_model,
            content: content_chunks.concat(),
            reasoning: reasoning_chunks.concat(),
            stop_reason,
            usage,
        })
    }

    async fn get_daemon(&self, autonomous: bool, sandbox: bool) -> Result<Arc<AcpDaemon>, GatewayError> {
        let daemon = {
            let mut guard = self.daemons.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .entry((autonomous, sandbox))
                .or_insert_with(|| AcpDaemon::new(self.settings.clone(), autonomous, sandbox))
                .clone()
        };
        daemon.ensure_started().await?;
        daemon
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_activity = Instant::now();
        Ok(daemon)
    }

    pub fn validate_folder(&self, folder: &str) -> Result<PathBuf, GatewayError> {
        let raw = folder.trim();
        if raw.is_empty() {
            return Err(GatewayError::FolderAccess("Folder must not be empty".into()));
        }
        let expanded = expand_tilde(Path::new(raw));
        if !expanded.is_absolute() {
            return Err(GatewayError::FolderAccess(format!(
                "Folder must be an absolute path: {folder}"
            )));
        }
        let resolved = std::fs::canonicalize(&expanded).map_err(|_| {
            GatewayError::FolderAccess(format!(
                "Folder does not exist or is not a directory: {}",
                expanded.display()
            ))
        })?;
        if !resolved.is_dir() {
            return Err(GatewayError::FolderAccess(format!(
                "Folder does not exist or is not a directory: {}",
                resolved.display()
            )));
        }

        if !self.settings.allowed_base_dirs.is_empty() {
            let allowed_roots: Vec<PathBuf> = self
                .settings
                .allowed_base_dirs
                .iter()
                .map(|p| {
                    let exp = expand_tilde(p);
                    std::fs::canonicalize(&exp).unwrap_or(exp)
                })
                .collect();
            let ok = allowed_roots
                .iter()
                .any(|root| resolved == *root || resolved.starts_with(root));
            if !ok {
                let roots_str: Vec<String> = allowed_roots
                    .iter()
                    .map(|r| r.display().to_string())
                    .collect();
                return Err(GatewayError::FolderAccess(format!(
                    "Folder {} is outside allowed_base_dirs: {:?}",
                    resolved.display(),
                    roots_str
                )));
            }
        }

        Ok(resolved)
    }

    fn get_or_create_lock(&self, conversation_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .entry(conversation_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn maybe_cleanup_lock(&self, conversation_id: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        let mut guard = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::strong_count(&lock) == 2 {
            guard.remove(conversation_id);
        }
    }

    async fn apply_config(
        &self,
        daemon: &Arc<AcpDaemon>,
        session_id: &str,
        model: Option<&str>,
        effort: Option<&str>,
        config_options: Option<&[Value]>,
    ) -> Result<(Option<String>, Option<String>), GatewayError> {
        let (mut session_opts, daemon_opts) = {
            let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
            let s_opts = config_options
                .map(|v| v.to_vec())
                .or_else(|| st.session_config_options.get(session_id).cloned())
                .unwrap_or_default();
            (s_opts, st.config_options.clone())
        };
        let schema_opts = if session_opts.is_empty() {
            &daemon_opts
        } else {
            &session_opts
        };
        let (resolved_model, resolved_effort) =
            resolve_model_and_effort(model, effort, schema_opts);

        if let Some(ref rm) = resolved_model {
            let model_opt = find_select_option(&session_opts, "model", "model");
            let current = model_opt
                .and_then(|o| o.get("currentValue"))
                .and_then(Value::as_str);
            if current != Some(rm.as_str()) {
                let cfg_id = model_opt
                    .and_then(|o| o.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("model");
                let resp = daemon
                    .call(
                        "session/set_config_option",
                        json!({
                            "configId": cfg_id,
                            "sessionId": session_id,
                            "value": rm,
                        }),
                        Some(Duration::from_secs(15)),
                    )
                    .await?;
                if let Some(opts) = resp.get("configOptions").and_then(Value::as_array) {
                    session_opts = opts.clone();
                    daemon.record_config_options(session_id, &session_opts);
                }
            }
        }

        if let Some(ref re) = resolved_effort {
            let effort_opt =
                find_select_option(&session_opts, "thought_level", "thought_level");
            let current = effort_opt
                .and_then(|o| o.get("currentValue"))
                .and_then(Value::as_str);
            if current != Some(re.as_str()) {
                let cfg_id = effort_opt
                    .and_then(|o| o.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("thought_level");
                let resp = daemon
                    .call(
                        "session/set_config_option",
                        json!({
                            "configId": cfg_id,
                            "sessionId": session_id,
                            "value": re,
                        }),
                        Some(Duration::from_secs(15)),
                    )
                    .await?;
                if let Some(opts) = resp.get("configOptions").and_then(Value::as_array) {
                    daemon.record_config_options(session_id, opts);
                }
            }
        }

        Ok((resolved_model, resolved_effort))
    }

    pub async fn start_session(
        &self,
        req: StartSessionRequest,
        on_event: Option<EventSender>,
        on_update: Option<UpdateSender>,
    ) -> Result<TurnResult, GatewayError> {
        let folder = self.validate_folder(&req.folder)?;
        let model = req.model.or_else(|| self.settings.default_model.clone());
        let effort = req.effort.or_else(|| self.settings.default_effort.clone());
        let autonomous = req.autonomous.unwrap_or(self.settings.default_autonomous);
        let sandbox = req.sandbox.unwrap_or(self.settings.default_sandbox);
        let timeout_seconds = req
            .timeout_seconds
            .unwrap_or(self.settings.default_timeout_seconds);

        let daemon = self.get_daemon(autonomous, sandbox).await?;
        let _inflight = daemon.track_inflight();

        let session_resp = daemon
            .call(
                "session/new",
                json!({
                    "cwd": folder.to_string_lossy(),
                    "mcpServers": []
                }),
                Some(Duration::from_secs(30)),
            )
            .await?;

        let conv_id = session_resp
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                GatewayError::gateway(format!("ACP session/new missing sessionId: {session_resp}"))
            })?
            .to_string();

        let config_options = session_resp
            .get("configOptions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        daemon.record_config_options(&conv_id, &config_options);
        daemon.record_session_models(&session_resp);

        let (resolved_model, resolved_effort) = match self
            .apply_config(
                &daemon,
                &conv_id,
                model.as_deref(),
                effort.as_deref(),
                Some(&config_options),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                daemon.close_session(&conv_id).await;
                return Err(e);
            }
        };

        let effective_model = resolved_model.or_else(|| {
            find_select_option(&config_options, "model", "model")
                .and_then(|o| o.get("currentValue"))
                .and_then(Value::as_str)
                .map(String::from)
        });

        let lock = self.get_or_create_lock(&conv_id);
        let res = {
            let _lock_guard = lock.lock().await;
            {
                let mut st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                st.active_sessions.insert(conv_id.clone(), Instant::now());
            }
            self.store
                .create(
                    &conv_id,
                    &folder.to_string_lossy(),
                    effective_model.as_deref(),
                    resolved_effort.as_deref(),
                    autonomous,
                    sandbox,
                    "running",
                )
                .map_err(|e| GatewayError::gateway(format!("DB error: {e}")))?;

            self.execute_turn(
                &daemon,
                &conv_id,
                &req.prompt,
                0,
                timeout_seconds,
                on_event,
                on_update,
                true,
            )
            .await
        };
        self.maybe_cleanup_lock(&conv_id, lock);
        res
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn continue_session(
        &self,
        conversation_id: &str,
        prompt: &str,
        model: Option<&str>,
        effort: Option<&str>,
        timeout_seconds: Option<u64>,
        on_event: Option<EventSender>,
        on_update: Option<UpdateSender>,
    ) -> Result<TurnResult, GatewayError> {
        let lock = self.get_or_create_lock(conversation_id);
        let res = {
            let _lock_guard = lock.lock().await;
            let session = self
                .store
                .get(conversation_id)
                .map_err(|e| GatewayError::gateway(format!("DB error: {e}")))?
                .ok_or_else(|| {
                    GatewayError::SessionNotFound(format!(
                        "Unknown conversation_id: {conversation_id}"
                    ))
                })?;

            let folder = self.validate_folder(&session.folder)?;
            let target_model = model.map(String::from).or_else(|| session.model.clone());
            let effective_timeout =
                timeout_seconds.unwrap_or(self.settings.default_timeout_seconds);

            let daemon = self.get_daemon(session.autonomous, session.sandbox).await?;
            let _inflight = daemon.track_inflight();

            let mut resumed_config: Option<Vec<Value>> = None;
            let mut need_apply = model.is_some() || effort.is_some();

            let is_active = daemon
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .active_sessions
                .contains_key(conversation_id);

            if !is_active {
                let resume_params = json!({
                    "sessionId": conversation_id,
                    "cwd": folder.to_string_lossy(),
                    "mcpServers": []
                });
                let resumed = if daemon.supports_resume() == Some(false) {
                    daemon
                        .call(
                            "session/load",
                            resume_params,
                            Some(Duration::from_secs(30)),
                        )
                        .await?
                } else {
                    match daemon
                        .call(
                            "session/resume",
                            resume_params.clone(),
                            Some(Duration::from_secs(30)),
                        )
                        .await
                    {
                        Ok(v) => {
                            daemon.set_supports_resume(true);
                            v
                        }
                        Err(err) => {
                            let is_not_found = err.code() == Some(-32601)
                                || err.to_string().to_ascii_lowercase().contains("method not found");
                            if !is_not_found {
                                return Err(err);
                            }
                            daemon.set_supports_resume(false);
                            daemon
                                .call(
                                    "session/load",
                                    resume_params,
                                    Some(Duration::from_secs(30)),
                                )
                                .await?
                        }
                    }
                };

                let cfg = resumed
                    .get("configOptions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                daemon.record_config_options(conversation_id, &cfg);
                daemon
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .active_sessions
                    .insert(conversation_id.to_string(), Instant::now());
                resumed_config = Some(cfg);
                need_apply = true;
            }

            let mut already_running = false;
            if need_apply
                && (target_model.is_some() || effort.is_some() || session.effort.is_some())
            {
                let opts = if let Some(ref rc) = resumed_config {
                    rc.clone()
                } else {
                    let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                    st.session_config_options
                        .get(conversation_id)
                        .cloned()
                        .unwrap_or_else(|| st.config_options.clone())
                };
                let (resolved_model, inferred_or_explicit_effort) =
                    resolve_model_and_effort(target_model.as_deref(), effort, &opts);
                let target_effort = inferred_or_explicit_effort.or_else(|| session.effort.clone());

                let (final_model, final_effort) = self
                    .apply_config(
                        &daemon,
                        conversation_id,
                        resolved_model.as_deref(),
                        target_effort.as_deref(),
                        Some(&opts),
                    )
                    .await?;

                if model.is_some() || effort.is_some() {
                    let _ = self.store.update(
                        conversation_id,
                        SessionUpdate {
                            model: final_model.as_deref(),
                            effort: final_effort.as_deref(),
                            status: Some("running"),
                            clear_error: true,
                            ..Default::default()
                        },
                    );
                    already_running = true;
                }
            }

            self.execute_turn(
                &daemon,
                conversation_id,
                prompt,
                session.num_turns,
                effective_timeout,
                on_event,
                on_update,
                already_running,
            )
            .await
        };
        self.maybe_cleanup_lock(conversation_id, lock);
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_turn(
        &self,
        daemon: &Arc<AcpDaemon>,
        conversation_id: &str,
        prompt: &str,
        prior_turns: i64,
        timeout_seconds: u64,
        on_event: Option<EventSender>,
        on_update: Option<UpdateSender>,
        already_running: bool,
    ) -> Result<TurnResult, GatewayError> {
        {
            self.externally_cancelled
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(conversation_id);
        }
        if !already_running {
            let _ = self.store.update(
                conversation_id,
                SessionUpdate {
                    status: Some("running"),
                    clear_error: true,
                    ..Default::default()
                },
            );
        }

        let t0 = Instant::now();
        let conn = daemon.conn()?;
        let (update_tx, mut update_rx) = mpsc::unbounded_channel::<Value>();
        conn.register_listener(conversation_id, update_tx);

        // Drop guard ensures that if this future is aborted/cancelled mid-turn,
        // we notify ACP session/cancel and mark the session cancelled in SQLite.
        struct CancelDropGuard {
            armed: bool,
            conn: Arc<AcpConnection>,
            store: Arc<SessionStore>,
            externally_cancelled: Arc<Mutex<HashSet<String>>>,
            conversation_id: String,
        }
        impl Drop for CancelDropGuard {
            fn drop(&mut self) {
                self.conn.unregister_listener(&self.conversation_id);
                if self.armed {
                    let _ = self.conn.notify(
                        "session/cancel",
                        json!({ "sessionId": self.conversation_id }),
                    );
                    self.externally_cancelled
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&self.conversation_id);
                    let _ = self.store.update(
                        &self.conversation_id,
                        SessionUpdate {
                            status: Some("cancelled"),
                            ..Default::default()
                        },
                    );
                }
            }
        }

        let mut drop_guard = CancelDropGuard {
            armed: true,
            conn: conn.clone(),
            store: self.store.clone(),
            externally_cancelled: self.externally_cancelled.clone(),
            conversation_id: conversation_id.to_string(),
        };

        let daemon_for_updates = daemon.clone();
        let conv_id_owned = conversation_id.to_string();
        let on_event_clone = on_event.clone();
        let update_collector = tokio::spawn(async move {
            let mut chunks: Vec<String> = Vec::new();
            let mut usage: Option<Value> = None;
            while let Some(update) = update_rx.recv().await {
                let now = Instant::now();
                {
                    let mut st = daemon_for_updates
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    st.last_activity = now;
                    if let Some(ts) = st.active_sessions.get_mut(&conv_id_owned) {
                        *ts = now;
                    }
                }

                let kind = update
                    .get("sessionUpdate")
                    .and_then(Value::as_str)
                    .unwrap_or("update");

                if kind == "agent_message_chunk" {
                    if update.pointer("/content/type").and_then(Value::as_str) == Some("text") {
                        if let Some(text) = update.pointer("/content/text").and_then(Value::as_str)
                        {
                            chunks.push(text.to_string());
                        }
                    }
                } else if kind == "usage_update" {
                    usage = Some(update.clone());
                }

                if let Some(ref tx) = on_update {
                    let _ = tx.send(update.clone());
                }
                if let Some(ref tx) = on_event_clone {
                    let _ = tx.send(json!({
                        "event": kind,
                        "conversation_id": conv_id_owned,
                        "update": update,
                    }));
                }
            }
            (chunks, usage)
        });

        let timeout = if timeout_seconds > 0 {
            Some(Duration::from_secs(timeout_seconds))
        } else {
            None
        };

        let prompt_res = daemon
            .call(
                "session/prompt",
                json!({
                    "sessionId": conversation_id,
                    "prompt": [{ "type": "text", "text": prompt }]
                }),
                timeout,
            )
            .await;

        // Disarm cancel-on-drop now that the prompt call settled; we handle errors explicitly below.
        drop_guard.armed = false;
        conn.unregister_listener(conversation_id);
        drop(drop_guard);

        let res = match prompt_res {
            Ok(v) => v,
            Err(GatewayError::TurnTimeout(msg)) => {
                let _ = conn.notify(
                    "session/cancel",
                    json!({ "sessionId": conversation_id }),
                );
                self.externally_cancelled
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(conversation_id);
                let _ = self.store.update(
                    conversation_id,
                    SessionUpdate {
                        status: Some("error"),
                        last_error: Some(&msg),
                        ..Default::default()
                    },
                );
                update_collector.abort();
                return Err(GatewayError::TurnTimeout(msg));
            }
            Err(err) => {
                let ext = self
                    .externally_cancelled
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(conversation_id);
                if ext {
                    let _ = self.store.update(
                        conversation_id,
                        SessionUpdate {
                            status: Some("cancelled"),
                            ..Default::default()
                        },
                    );
                } else {
                    let msg = err.to_string();
                    let _ = self.store.update(
                        conversation_id,
                        SessionUpdate {
                            status: Some("error"),
                            last_error: Some(&msg),
                            ..Default::default()
                        },
                    );
                }
                update_collector.abort();
                return Err(err);
            }
        };

        // Wait for collector to drain all queued session/update messages now that listener is unregistered.
        let (chunks, mut usage) = update_collector.await.unwrap_or_default();

        let duration = (t0.elapsed().as_secs_f64() * 10000.0).round() / 10000.0;
        let stop_reason = res
            .get("stopReason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn")
            .to_string();
        if let Some(u) = res.get("usage").filter(|v| !v.is_null()) {
            usage = Some(u.clone());
        }

        let ext = self
            .externally_cancelled
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(conversation_id);
        let is_cancelled = stop_reason == "cancelled" || ext;
        let status = if is_cancelled { "CANCELLED" } else { "SUCCESS" }.to_string();
        let num_turns = prior_turns + 1;

        let result = TurnResult {
            conversation_id: conversation_id.to_string(),
            status: status.clone(),
            response: chunks.concat(),
            error: None,
            duration_seconds: Some(duration),
            num_turns: Some(num_turns),
            usage,
            finished_reason: Some(stop_reason),
        };

        let now = Instant::now();
        {
            let mut st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
            st.last_activity = now;
            if let Some(ts) = st.active_sessions.get_mut(conversation_id) {
                *ts = now;
            }
        }
        let _ = self.store.update(
            conversation_id,
            SessionUpdate {
                status: Some(if is_cancelled { "cancelled" } else { "idle" }),
                num_turns: Some(num_turns),
                clear_error: true,
                ..Default::default()
            },
        );

        if let Some(tx) = on_event {
            let _ = tx.send(json!({
                "event": "result",
                "result": result,
            }));
        }

        Ok(result)
    }

    pub async fn cancel_session(&self, conversation_id: &str) -> Result<bool, GatewayError> {
        let session = self
            .store
            .get(conversation_id)
            .map_err(|e| GatewayError::gateway(format!("DB error: {e}")))?
            .ok_or_else(|| {
                GatewayError::SessionNotFound(format!(
                    "Unknown conversation_id: {conversation_id}"
                ))
            })?;

        let daemon = match self.daemon_for(session.autonomous, session.sandbox) {
            Some(d) => d,
            None => return Ok(false),
        };

        let active = {
            let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
            st.initialized
                && st.conn.as_ref().is_some_and(|c| c.is_alive())
                && st.active_sessions.contains_key(conversation_id)
        };
        if !active {
            return Ok(false);
        }

        let lock = self.get_or_create_lock(conversation_id);
        let stopped = match lock.try_lock() {
            Ok(_guard) => {
                daemon.close_session(conversation_id).await;
                let _ = self.store.update(
                    conversation_id,
                    SessionUpdate {
                        status: Some("cancelled"),
                        ..Default::default()
                    },
                );
                true
            }
            Err(_) => {
                if let Ok(conn) = daemon.conn() {
                    let _ = conn.notify(
                        "session/cancel",
                        json!({ "sessionId": conversation_id }),
                    );
                }
                match tokio::time::timeout(Duration::from_secs(5), lock.lock()).await {
                    Ok(_guard) => {
                        daemon.close_session(conversation_id).await;
                        let _ = self.store.update(
                            conversation_id,
                            SessionUpdate {
                                status: Some("cancelled"),
                                ..Default::default()
                            },
                        );
                        true
                    }
                    Err(_) => {
                        self.externally_cancelled
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(conversation_id.to_string());
                        daemon.close_session(conversation_id).await;
                        let _ = self.store.update(
                            conversation_id,
                            SessionUpdate {
                                status: Some("cancelled"),
                                ..Default::default()
                            },
                        );
                        true
                    }
                }
            }
        };
        self.maybe_cleanup_lock(conversation_id, lock);
        Ok(stopped)
    }

    pub fn get_session(&self, conversation_id: &str) -> Result<SessionInfo, GatewayError> {
        let session = self
            .store
            .get(conversation_id)
            .map_err(|e| GatewayError::gateway(format!("DB error: {e}")))?
            .ok_or_else(|| {
                GatewayError::SessionNotFound(format!(
                    "Unknown conversation_id: {conversation_id}"
                ))
            })?;
        Ok(self.enrich(session))
    }

    pub fn list_sessions(&self) -> Vec<SessionInfo> {
        self.store
            .list_all()
            .unwrap_or_default()
            .into_iter()
            .map(|s| self.enrich(s))
            .collect()
    }

    fn enrich(&self, mut session: SessionInfo) -> SessionInfo {
        let warm = self
            .daemon_for(session.autonomous, session.sandbox)
            .is_some_and(|d| {
                let st = d.state.lock().unwrap_or_else(|e| e.into_inner());
                st.initialized
                    && st.conn.as_ref().is_some_and(|c| c.is_alive())
                    && st.active_sessions.contains_key(&session.conversation_id)
            });
        let busy = self
            .locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&session.conversation_id)
            .is_some_and(|l| l.try_lock().is_err());

        session.warm = warm;
        session.busy = busy;
        session
    }

    pub async fn list_models(&self) -> Result<Vec<ModelInfo>, GatewayError> {
        {
            let guard = self.daemons.lock().unwrap_or_else(|e| e.into_inner());
            for daemon in guard.values() {
                let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                if !st.available_models.is_empty() {
                    return Ok(st.available_models.clone());
                }
            }
        }
        if let Some(cd) = self
            .completions_daemon
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            let st = cd.state.lock().unwrap_or_else(|e| e.into_inner());
            if !st.available_models.is_empty() {
                return Ok(st.available_models.clone());
            }
        }

        let Some((program, args)) = self.settings.models_command.split_first() else {
            return Ok(Vec::new());
        };

        let mut command = Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(child_env())
            .kill_on_drop(true);

        let output = match tokio::time::timeout(Duration::from_secs(30), command.output()).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                return Err(GatewayError::gateway(format!(
                    "Failed to run models command ({program}): {e}"
                )));
            }
            Err(_) => {
                return Err(GatewayError::gateway(
                    "Models command timed out after 30s",
                ));
            }
        };

        if !output.status.success() {
            let rc = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::gateway(format!(
                "Models command failed (rc={rc}): {stderr}"
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut models = Vec::new();
        for raw_line in stdout.split('\n') {
            let last_cr = raw_line.rsplit('\r').next().unwrap_or(raw_line);
            let cleaned = ansi_re().replace_all(last_cr, "");
            let cleaned = cleaned.trim();
            if cleaned.is_empty() || cleaned.contains("Fetching available models") {
                continue;
            }
            let mut parts = cleaned.splitn(2, char::is_whitespace);
            if let (Some(id), Some(rest)) = (parts.next(), parts.next()) {
                let label = rest.trim();
                if !label.is_empty() && model_id_re().is_match(id) {
                    models.push(ModelInfo {
                        id: id.to_string(),
                        label: label.to_string(),
                    });
                }
            }
        }
        Ok(models)
    }

    async fn reaper_loop(&self) {
        let session_ttl = self.settings.idle_ttl_seconds;
        let daemon_ttl = self.settings.daemon_idle_ttl_seconds;
        let active_ttls: Vec<u64> = [session_ttl, daemon_ttl]
            .into_iter()
            .filter(|&t| t > 0)
            .collect();
        let Some(&min_ttl) = active_ttls.iter().min() else {
            return;
        };
        let third = if min_ttl / 3 == 0 { 5 } else { min_ttl / 3 };
        let interval = Duration::from_secs(third.clamp(5, 60));

        loop {
            tokio::time::sleep(interval).await;
            let mut daemons: Vec<Arc<AcpDaemon>> = {
                self.daemons
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .values()
                    .cloned()
                    .collect()
            };
            if let Some(cd) = self
                .completions_daemon
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                daemons.push(cd);
            }

            let now = Instant::now();
            for daemon in daemons {
                if !daemon.is_alive() {
                    continue;
                }
                if session_ttl > 0 {
                    let candidates: Vec<String> = {
                        let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                        st.active_sessions
                            .iter()
                            .filter(|(_, ts)| now.duration_since(**ts).as_secs() >= session_ttl)
                            .map(|(k, _)| k.clone())
                            .collect()
                    };
                    for conv_id in candidates {
                        let existing_locked = self
                            .locks
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get(&conv_id)
                            .is_some_and(|l| l.try_lock().is_err());
                        if existing_locked {
                            continue;
                        }
                        let lock = self.get_or_create_lock(&conv_id);
                        {
                            let _guard = lock.lock().await;
                            let expired = {
                                let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                                st.active_sessions
                                    .get(&conv_id)
                                    .is_some_and(|ts| ts.elapsed().as_secs() >= session_ttl)
                            };
                            if expired {
                                daemon.close_session(&conv_id).await;
                            }
                        }
                        self.maybe_cleanup_lock(&conv_id, lock);
                    }
                }

                if daemon_ttl > 0 {
                    let should_check = {
                        let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                        st.active_sessions.is_empty()
                            && st.inflight == 0
                            && st.last_activity.elapsed().as_secs() >= daemon_ttl
                    };
                    if should_check {
                        let _start_guard = daemon.start_lock.lock().await;
                        let still_idle = {
                            let st = daemon.state.lock().unwrap_or_else(|e| e.into_inner());
                            st.initialized
                                && st.conn.as_ref().is_some_and(|c| c.is_alive())
                                && st.active_sessions.is_empty()
                                && st.inflight == 0
                                && st.last_activity.elapsed().as_secs() >= daemon_ttl
                        };
                        if still_idle {
                            daemon.shutdown().await;
                        }
                    }
                }
            }
        }
    }
}
