use std::path::PathBuf;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::config::expand_tilde;
use crate::models::SessionInfo;

fn utc_now() -> String {
    Utc::now().to_rfc3339()
}

pub struct SessionStore {
    db_path: PathBuf,
    conn: Mutex<Option<Connection>>,
}

#[derive(Default)]
pub struct SessionUpdate<'a> {
    pub model: Option<&'a str>,
    pub effort: Option<&'a str>,
    pub status: Option<&'a str>,
    pub num_turns: Option<i64>,
    pub last_error: Option<&'a str>,
    pub clear_error: bool,
}

impl SessionStore {
    pub fn new(db_path: PathBuf) -> Result<Self, rusqlite::Error> {
        let db_path = expand_tilde(&db_path);
        if let Some(parent) = db_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let store = Self {
            db_path,
            conn: Mutex::new(None),
        };
        store.init_db()?;
        Ok(store)
    }

    fn with_conn<F, T>(&self, f: F) -> Result<T, rusqlite::Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error>,
    {
        let mut guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            if let Some(parent) = self.db_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let conn = Connection::open(&self.db_path)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            conn.pragma_update(None, "busy_timeout", 5000)?;
            *guard = Some(conn);
        }
        let conn = guard.as_mut().expect("connection initialized");
        f(conn)
    }

    pub fn close(&self) {
        let mut guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        *guard = None;
    }

    fn init_db(&self) -> Result<(), rusqlite::Error> {
        self.with_conn(|conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS sessions (
                    conversation_id TEXT PRIMARY KEY,
                    folder TEXT NOT NULL,
                    model TEXT,
                    effort TEXT,
                    autonomous INTEGER NOT NULL DEFAULT 1,
                    sandbox INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'idle',
                    num_turns INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT
                );",
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        conversation_id: &str,
        folder: &str,
        model: Option<&str>,
        effort: Option<&str>,
        autonomous: bool,
        sandbox: bool,
        status: &str,
    ) -> Result<SessionInfo, rusqlite::Error> {
        let now = utc_now();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO sessions (
                    conversation_id, folder, model, effort, autonomous, sandbox,
                    status, num_turns, created_at, updated_at, last_error
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9, NULL)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    folder=excluded.folder,
                    model=COALESCE(excluded.model, sessions.model),
                    effort=COALESCE(excluded.effort, sessions.effort),
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    last_error=NULL",
                params![
                    conversation_id,
                    folder,
                    model,
                    effort,
                    i64::from(autonomous),
                    i64::from(sandbox),
                    status,
                    now,
                    now,
                ],
            )?;
            conn.query_row(
                "SELECT * FROM sessions WHERE conversation_id = ?1",
                params![conversation_id],
                row_to_model,
            )
        })
    }

    pub fn update(
        &self,
        conversation_id: &str,
        upd: SessionUpdate<'_>,
    ) -> Result<(), rusqlite::Error> {
        let now = utc_now();
        let mut fields: Vec<String> = vec!["updated_at = ?".into()];
        let mut values: Vec<rusqlite::types::Value> = vec![now.into()];

        if let Some(m) = upd.model {
            fields.push("model = ?".into());
            values.push(m.to_string().into());
        }
        if let Some(e) = upd.effort {
            fields.push("effort = ?".into());
            values.push(e.to_string().into());
        }
        if let Some(s) = upd.status {
            fields.push("status = ?".into());
            values.push(s.to_string().into());
        }
        if let Some(n) = upd.num_turns {
            fields.push("num_turns = ?".into());
            values.push(n.into());
        }
        if upd.clear_error {
            fields.push("last_error = NULL".into());
        } else if let Some(err) = upd.last_error {
            fields.push("last_error = ?".into());
            values.push(err.to_string().into());
        }

        values.push(conversation_id.to_string().into());
        let sql = format!(
            "UPDATE sessions SET {} WHERE conversation_id = ?",
            fields.join(", ")
        );
        self.with_conn(|conn| {
            conn.execute(&sql, rusqlite::params_from_iter(values))?;
            Ok(())
        })
    }

    pub fn reset_stale_running(&self) -> Result<usize, rusqlite::Error> {
        let now = utc_now();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE sessions
                 SET status = 'error',
                     last_error = COALESCE(last_error, 'Interrupted by server restart'),
                     updated_at = ?1
                 WHERE status = 'running'",
                params![now],
            )
        })
    }

    pub fn get(&self, conversation_id: &str) -> Result<Option<SessionInfo>, rusqlite::Error> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT * FROM sessions WHERE conversation_id = ?1",
                params![conversation_id],
                row_to_model,
            )
            .optional()
        })
    }

    pub fn list_all(&self) -> Result<Vec<SessionInfo>, rusqlite::Error> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT * FROM sessions ORDER BY updated_at DESC")?;
            let rows = stmt.query_map([], row_to_model)?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r?);
            }
            Ok(out)
        })
    }
}

fn row_to_model(row: &Row<'_>) -> rusqlite::Result<SessionInfo> {
    let autonomous: i64 = row.get("autonomous")?;
    let sandbox: i64 = row.get("sandbox")?;
    Ok(SessionInfo {
        conversation_id: row.get("conversation_id")?,
        folder: row.get("folder")?,
        model: row.get("model")?,
        effort: row.get("effort")?,
        autonomous: autonomous != 0,
        sandbox: sandbox != 0,
        status: row.get("status")?,
        num_turns: row.get("num_turns")?,
        warm: false,
        busy: false,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        last_error: row.get("last_error")?,
    })
}
