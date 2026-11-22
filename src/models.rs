use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StartSessionRequest {
    pub folder: String,
    pub prompt: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub autonomous: Option<bool>,
    #[serde(default)]
    pub sandbox: Option<bool>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub stream: bool,
}

impl StartSessionRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.folder.is_empty() {
            return Err("folder must not be empty".to_string());
        }
        if self.prompt.is_empty() {
            return Err("prompt must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContinueSessionRequest {
    pub prompt: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub stream: bool,
}

impl ContinueSessionRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.prompt.is_empty() {
            return Err("prompt must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnResult {
    pub conversation_id: String,
    pub status: String,
    pub response: String,
    pub error: Option<String>,
    pub duration_seconds: Option<f64>,
    pub num_turns: Option<i64>,
    pub usage: Option<Value>,
    pub finished_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub conversation_id: String,
    pub folder: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub autonomous: bool,
    pub sandbox: bool,
    pub status: String,
    pub num_turns: i64,
    pub warm: bool,
    pub busy: bool,
    pub created_at: String,
    pub updated_at: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub label: String,
}
