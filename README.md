# acp-gateway

Unified **MCP Streamable HTTP** and **REST/SSE** gateway written in Rust, wrapping any **ACP (Agent Client Protocol)** coding agent (defaults to `agy acp`) on a single shared warm `SessionManager` pool.

- Single warm ACP process multiplexes sessions across folders and clients (MCP + REST share the same warm daemon).
- Native async Rust (`tokio` + `axum` + bundled `rusqlite`) speaking ACP JSON-RPC 2.0 over stdio; agent-agnostic command/flag configuration.
- Forwards ACP `session/update` lifecycle events (`tool_call`, `tool_call_update`, `plan`, `usage_update`, throttled chunk heartbeats) to MCP progress notifications to prevent client timeouts on long turns.

## Build & Run

```bash
cargo build --release
./target/release/acp-gateway
```

Or run tests with Docker:

```bash
docker build --target test -t acp-gateway-test .
docker run --rm acp-gateway-test
```

Serves REST/SSE at `http://127.0.0.1:8765`, OpenAI-compatible Chat Completions at `http://127.0.0.1:8765/v1`, and MCP Streamable HTTP at `http://127.0.0.1:8765/mcp`.

## Connect to Hermes Agent (MCP Streamable HTTP)

Add to `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  acp:
    url: http://127.0.0.1:8765/mcp
```

Tools exposed: `acp_start`, `acp_continue`, `acp_list_sessions`, `acp_get_session`, `acp_cancel`, `acp_models`.

## REST / SSE Usage

```bash
curl -s http://127.0.0.1:8765/sessions \
  -H "Content-Type: application/json" \
  -d '{"folder":"/Users/rw/devspace/my-repo","prompt":"Summarize this repo"}'

curl -s http://127.0.0.1:8765/sessions/<conversation_id>/messages \
  -H "Content-Type: application/json" \
  -d '{"prompt":"Now add unit tests","stream":true}'
```

## OpenAI-Compatible API (`/v1/models`, `/v1/chat/completions`)

Runs stateless, ephemeral completions on a dedicated non-autonomous ACP daemon (`autonomous=false`, isolated empty `cwd`, host tool permissions denied) suitable for LiteLLM / OpenAI SDK clients:

```bash
curl -s http://127.0.0.1:8765/v1/models
curl -s http://127.0.0.1:8765/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gemini-4-neon-medium","messages":[{"role":"user","content":"Hello"}],"stream":false}'
```

## Configuration (`ACP_GATEWAY_*`)

| Env Var | Default | Purpose |
|---|---|---|
| `ACP_GATEWAY_ACP_COMMAND` | `["agy", "acp"]` | ACP agent command (JSON array or shell string) |
| `ACP_GATEWAY_AUTONOMOUS_FLAGS` | `["--dangerously-skip-permissions"]` | CLI flags appended when `autonomous=true` |
| `ACP_GATEWAY_SANDBOX_FLAGS` | `["--sandbox"]` | CLI flags appended when `sandbox=true` |
| `ACP_GATEWAY_MODELS_COMMAND` | `["agy", "models"]` | Optional fallback CLI command to list models (empty disables) |
| `ACP_GATEWAY_DB_PATH` | `~/.local/share/acp-gateway/sessions.db` | SQLite session registry |
| `ACP_GATEWAY_ALLOWED_BASE_DIRS` | `[]` | JSON list or comma-separated allowed root folders |
| `ACP_GATEWAY_DEFAULT_MODEL` | `None` | Default model ID |
| `ACP_GATEWAY_DEFAULT_EFFORT` | `None` | Default reasoning effort / thought level |
| `ACP_GATEWAY_DEFAULT_AUTONOMOUS` | `true` | Auto-approve permissions + pass `autonomous_flags` |
| `ACP_GATEWAY_DEFAULT_SANDBOX` | `false` | Pass `sandbox_flags` |
| `ACP_GATEWAY_DEFAULT_TIMEOUT_SECONDS` | `0` | Per-turn timeout in seconds (0 = unlimited) |
| `ACP_GATEWAY_IDLE_TTL_SECONDS` | `900` | Idle session reaping timeout (0 = disabled) |
| `ACP_GATEWAY_DAEMON_IDLE_TTL_SECONDS` | `0` | Idle daemon reaping timeout (0 = keep warm until shutdown) |
| `ACP_GATEWAY_PREWARM_DAEMON` | `true` | Pre-start default ACP daemon on gateway startup |
| `ACP_GATEWAY_HOST` / `PORT` / `MCP_PATH` | `127.0.0.1` / `8765` / `/mcp` | Bind address and MCP Streamable HTTP path |
| `ACP_GATEWAY_API_TOKEN` | `None` | Optional Bearer token for REST, `/v1/*`, and `/mcp` (except `/health`) |
| `ACP_GATEWAY_OPENAI_ACP_FLAGS` | `["--include-default-customizations=false"]` (for `agy`) | Extra ACP flags for the isolated `/v1/chat/completions` daemon |
| `ACP_GATEWAY_OPENAI_CWD` | `<db_dir>/openai-cwd` | Isolated empty working directory for `/v1/chat/completions` |
