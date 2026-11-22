use std::net::SocketAddr;

use acp_gateway::config::Settings;
use acp_gateway::http::create_app;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let settings = Settings::from_env()?;
    let addr: SocketAddr = format!("{}:{}", settings.host, settings.port).parse()?;
    let (app, manager) = create_app(settings);

    manager.start().await;
    tracing::info!("acp-gateway listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let shutdown_mgr = manager.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Shutting down acp-gateway...");
            shutdown_mgr.shutdown().await;
        })
        .await?;

    Ok(())
}
