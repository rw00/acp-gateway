use std::collections::HashMap;
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Invalid configuration for {0}: {1}")]
    Invalid(String, String),
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub acp_command: Vec<String>,
    pub autonomous_flags: Vec<String>,
    pub sandbox_flags: Vec<String>,
    pub models_command: Vec<String>,
    pub db_path: PathBuf,
    pub allowed_base_dirs: Vec<PathBuf>,
    pub default_model: Option<String>,
    pub default_effort: Option<String>,
    pub default_autonomous: bool,
    pub default_sandbox: bool,
    pub default_timeout_seconds: u64,
    pub idle_ttl_seconds: u64,
    pub daemon_idle_ttl_seconds: u64,
    pub prewarm_daemon: bool,
    pub host: String,
    pub port: u16,
    pub mcp_path: String,
    pub api_token: Option<String>,
    pub openai_acp_flags: Option<Vec<String>>,
    pub openai_cwd: Option<PathBuf>,
}

impl Default for Settings {
    fn default() -> Self {
        let db_path = home_dir()
            .join(".local")
            .join("share")
            .join("acp-gateway")
            .join("sessions.db");
        Self {
            acp_command: vec!["agy".into(), "acp".into()],
            autonomous_flags: vec!["--dangerously-skip-permissions".into()],
            sandbox_flags: vec!["--sandbox".into()],
            models_command: vec!["agy".into(), "models".into()],
            db_path,
            allowed_base_dirs: Vec::new(),
            default_model: None,
            default_effort: None,
            default_autonomous: true,
            default_sandbox: false,
            default_timeout_seconds: 0,
            idle_ttl_seconds: 900,
            daemon_idle_ttl_seconds: 0,
            prewarm_daemon: true,
            host: "127.0.0.1".into(),
            port: 8765,
            mcp_path: "/mcp".into(),
            api_token: None,
            openai_acp_flags: None,
            openai_cwd: None,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Self, ConfigError> {
        let _ = dotenvy::dotenv();
        let vars: HashMap<String, String> = std::env::vars().collect();
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let mut s = Self::default();
        let get = |key: &str| -> Option<&str> {
            vars.get(&format!("ACP_GATEWAY_{key}")).map(String::as_str)
        };

        if let Some(v) = get("ACP_COMMAND") {
            s.acp_command = parse_str_list(v, false)
                .map_err(|e| ConfigError::Invalid("acp_command".into(), e))?;
        }
        if let Some(v) = get("AUTONOMOUS_FLAGS") {
            s.autonomous_flags = parse_str_list(v, false)
                .map_err(|e| ConfigError::Invalid("autonomous_flags".into(), e))?;
        }
        if let Some(v) = get("SANDBOX_FLAGS") {
            s.sandbox_flags = parse_str_list(v, false)
                .map_err(|e| ConfigError::Invalid("sandbox_flags".into(), e))?;
        }
        if let Some(v) = get("MODELS_COMMAND") {
            s.models_command = parse_str_list(v, false)
                .map_err(|e| ConfigError::Invalid("models_command".into(), e))?;
        }
        if let Some(v) = get("DB_PATH") {
            if !v.trim().is_empty() {
                s.db_path = expand_tilde(Path::new(v.trim()));
            }
        }
        if let Some(v) = get("ALLOWED_BASE_DIRS") {
            s.allowed_base_dirs = parse_str_list(v, true)
                .map_err(|e| ConfigError::Invalid("allowed_base_dirs".into(), e))?
                .into_iter()
                .map(PathBuf::from)
                .collect();
        }
        if let Some(v) = get("DEFAULT_MODEL") {
            s.default_model = non_empty_opt(v);
        }
        if let Some(v) = get("DEFAULT_EFFORT") {
            s.default_effort = non_empty_opt(v);
        }
        if let Some(v) = get("DEFAULT_AUTONOMOUS") {
            s.default_autonomous = parse_bool(v)
                .map_err(|e| ConfigError::Invalid("default_autonomous".into(), e))?;
        }
        if let Some(v) = get("DEFAULT_SANDBOX") {
            s.default_sandbox = parse_bool(v)
                .map_err(|e| ConfigError::Invalid("default_sandbox".into(), e))?;
        }
        if let Some(v) = get("DEFAULT_TIMEOUT_SECONDS") {
            s.default_timeout_seconds = parse_non_neg_u64(v)
                .map_err(|e| ConfigError::Invalid("default_timeout_seconds".into(), e))?;
        }
        if let Some(v) = get("IDLE_TTL_SECONDS") {
            s.idle_ttl_seconds = parse_non_neg_u64(v)
                .map_err(|e| ConfigError::Invalid("idle_ttl_seconds".into(), e))?;
        }
        if let Some(v) = get("DAEMON_IDLE_TTL_SECONDS") {
            s.daemon_idle_ttl_seconds = parse_non_neg_u64(v)
                .map_err(|e| ConfigError::Invalid("daemon_idle_ttl_seconds".into(), e))?;
        }
        if let Some(v) = get("PREWARM_DAEMON") {
            s.prewarm_daemon = parse_bool(v)
                .map_err(|e| ConfigError::Invalid("prewarm_daemon".into(), e))?;
        }
        if let Some(v) = get("HOST") {
            if !v.trim().is_empty() {
                s.host = v.trim().to_string();
            }
        }
        if let Some(v) = get("PORT") {
            s.port = v
                .trim()
                .parse::<u16>()
                .map_err(|e| ConfigError::Invalid("port".into(), e.to_string()))?;
        }
        if let Some(v) = get("MCP_PATH") {
            s.mcp_path = v.trim().to_string();
        }
        if let Some(v) = get("API_TOKEN") {
            s.api_token = non_empty_opt(v);
        }
        if let Some(v) = get("OPENAI_ACP_FLAGS") {
            s.openai_acp_flags = Some(
                parse_str_list(v, false)
                    .map_err(|e| ConfigError::Invalid("openai_acp_flags".into(), e))?,
            );
        }
        if let Some(v) = get("OPENAI_CWD") {
            if !v.trim().is_empty() {
                s.openai_cwd = Some(expand_tilde(Path::new(v.trim())));
            }
        }

        s.validate()?;
        Ok(s)
    }

    pub fn resolved_openai_acp_flags(&self) -> Vec<String> {
        if let Some(ref explicit) = self.openai_acp_flags {
            return explicit.clone();
        }
        let is_agy = self
            .acp_command
            .first()
            .and_then(|cmd| Path::new(cmd).file_name())
            .and_then(|n| n.to_str())
            .is_some_and(|name| name == "agy");
        if is_agy
            && !self
                .acp_command
                .iter()
                .any(|a| a.starts_with("--include-default-customizations"))
        {
            vec!["--include-default-customizations=false".into()]
        } else {
            Vec::new()
        }
    }

    pub fn resolved_openai_cwd(&self) -> PathBuf {
        if let Some(ref cwd) = self.openai_cwd {
            return cwd.clone();
        }
        self.db_path
            .parent()
            .unwrap_or_else(|| Path::new("/tmp"))
            .join("openai-cwd")
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.acp_command.is_empty() {
            return Err(ConfigError::Invalid(
                "acp_command".into(),
                "acp_command must not be empty".into(),
            ));
        }
        if self.port == 0 {
            return Err(ConfigError::Invalid(
                "port".into(),
                "port must be between 1 and 65535".into(),
            ));
        }
        let path = self.mcp_path.trim();
        if !path.starts_with('/') {
            return Err(ConfigError::Invalid(
                "mcp_path".into(),
                "mcp_path must start with '/'".into(),
            ));
        }
        let reserved = ["/health", "/models", "/sessions", "/v1"];
        if reserved
            .iter()
            .any(|r| path == *r || path.starts_with(&format!("{r}/")))
        {
            return Err(ConfigError::Invalid(
                "mcp_path".into(),
                format!("mcp_path {path:?} collides with reserved REST route"),
            ));
        }
        Ok(())
    }
}

pub fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

pub fn expand_tilde(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if s == "~" {
        return home_dir();
    }
    if let Some(rest) = s.strip_prefix("~/") {
        return home_dir().join(rest);
    }
    path.to_path_buf()
}

fn non_empty_opt(v: &str) -> Option<String> {
    let trimmed = v.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_bool(v: &str) -> Result<bool, String> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean: {other}")),
    }
}

fn parse_non_neg_u64(v: &str) -> Result<u64, String> {
    v.trim()
        .parse::<u64>()
        .map_err(|e| format!("invalid non-negative integer: {e}"))
}

fn parse_str_list(v: &str, comma_split: bool) -> Result<Vec<String>, String> {
    let stripped = v.trim();
    if stripped.is_empty() {
        return Ok(Vec::new());
    }
    if stripped.starts_with('[') {
        return serde_json::from_str::<Vec<String>>(stripped)
            .map_err(|e| format!("invalid JSON list: {e}"));
    }
    if comma_split {
        return Ok(stripped
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect());
    }
    shlex::split(stripped).ok_or_else(|| "invalid shell quoting".to_string())
}
