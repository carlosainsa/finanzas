use anyhow::Result;
use tracing::info;

mod config;
mod executor;
mod metrics;
mod orderbook;
mod redis_client;
mod risk;
mod state_store;
mod ws_client;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    dotenv::dotenv().ok();

    info!("Starting Rust trading engine...");

    let config = config::Config::from_env()?;

    let ws_publisher = redis_client::StreamProducer::new(&config.redis_url).await?;
    let exec_publisher = redis_client::StreamProducer::new(&config.redis_url).await?;
    let exec_consumer = redis_client::StreamConsumer::new(
        &config.redis_url,
        "signals:stream",
        "rust-executor",
        "executor-1",
    )
    .await?;

    tokio::try_join!(
        ws_client::run(
            ws_publisher,
            config.polymarket_ws_url.clone(),
            config.market_asset_ids.clone()
        ),
        executor::run(exec_publisher, exec_consumer, config),
    )?;

    Ok(())
}
