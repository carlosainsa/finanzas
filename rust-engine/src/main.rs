use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod clob_client;
mod config;
mod control;
mod executor;
mod metrics;
mod orderbook;
mod reconciliation;
mod redis_client;
mod risk;
mod state_store;
mod ws_client;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    dotenv::dotenv().ok();

    info!("Starting Rust trading engine...");

    let config = config::Config::from_env()?;
    let order_tracker = reconciliation::OrderTracker::new();
    let reconciliation_config = config.clone();

    let ws_publisher = redis_client::StreamProducer::new(&config.redis_url).await?;
    let exec_publisher = redis_client::StreamProducer::new(&config.redis_url).await?;
    let reconciliation_publisher = redis_client::StreamProducer::new(&config.redis_url).await?;
    let control_publisher = redis_client::StreamProducer::new(&config.redis_url).await?;
    let exec_consumer = redis_client::StreamConsumer::new(
        &config.redis_url,
        &config.signals_stream,
        &config.executor_consumer_group,
        &config.executor_consumer_name,
    )
    .await?;
    let control_consumer = redis_client::StreamConsumer::new(
        &config.redis_url,
        &config.operator_commands_stream,
        &config.operator_consumer_group,
        &config.operator_consumer_name,
    )
    .await?;

    if config.disable_market_ws {
        info!("Market WebSocket disabled by DISABLE_MARKET_WS=true");
        tokio::try_join!(
            executor::run(exec_publisher, exec_consumer, config, order_tracker.clone()),
            control::run(
                control_publisher,
                control_consumer,
                reconciliation_config.clone(),
                order_tracker.clone()
            ),
            reconciliation::run(
                reconciliation_publisher,
                reconciliation_config,
                order_tracker.clone()
            ),
        )?;
    } else {
        tokio::try_join!(
            ws_client::run(
                ws_publisher,
                config.polymarket_ws_url.clone(),
                config.market_asset_ids.clone()
            ),
            executor::run(exec_publisher, exec_consumer, config, order_tracker.clone()),
            control::run(
                control_publisher,
                control_consumer,
                reconciliation_config.clone(),
                order_tracker.clone()
            ),
            reconciliation::run(
                reconciliation_publisher,
                reconciliation_config,
                order_tracker.clone()
            ),
        )?;
    }

    Ok(())
}
