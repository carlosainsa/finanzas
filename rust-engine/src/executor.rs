use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use polymarket_client_sdk::clob::types::{OrderStatusType, OrderType, Side as ClobSide};
use polymarket_client_sdk::types::{Decimal, U256};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::clob_client::AuthenticatedClob;
use crate::config::{Config, ExecutionMode};
use crate::metrics::Metrics;
use crate::reconciliation::OrderTracker;
use crate::redis_client::{KeyValueStore, StreamConsumer, StreamProducer};
use crate::risk::RiskService;
use crate::state_store::StateStore;

/// Alineado con shared/schemas/trade_signal.json
#[derive(Debug, Deserialize, Serialize)]
pub struct TradeSignal {
    pub signal_id: String,
    pub market_id: String,
    pub asset_id: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub confidence: f64,
    pub timestamp_ms: u64,
    pub source_timestamp_ms: Option<u64>,
    pub strategy: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

/// Alineado con shared/schemas/execution_report.json
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionReport {
    pub signal_id: String,
    pub order_id: String,
    pub status: ExecutionStatus,
    pub filled_price: Option<f64>,
    pub filled_size: Option<f64>,
    pub error: Option<String>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionStatus {
    Matched,
    Delayed,
    Unmatched,
    Cancelled,
    Error,
}

pub async fn run(
    mut publisher: StreamProducer,
    mut consumer: StreamConsumer,
    config: Config,
    order_tracker: OrderTracker,
) -> Result<()> {
    info!("Starting order executor...");

    let mut executor = OrderExecutor::new(config, order_tracker).await?;

    loop {
        let message = consumer.next_message().await?;
        let signal: TradeSignal = match serde_json::from_str(&message.payload) {
            Ok(signal) => signal,
            Err(err) => {
                error!(error = %err, raw = %message.payload, "Invalid trade signal JSON");
                publisher
                    .add_json(
                        "signals:deadletter",
                        &serde_json::json!({
                            "stream_id": message.id,
                            "error": err.to_string(),
                            "payload": message.payload,
                            "timestamp_ms": now_ms()
                        })
                        .to_string(),
                    )
                    .await?;
                consumer.ack(&message.id).await?;
                continue;
            }
        };

        info!(
            signal_id = %signal.signal_id,
            market_id = %signal.market_id,
            asset_id = %signal.asset_id,
            side = ?signal.side,
            price = signal.price,
            size = signal.size,
            confidence = signal.confidence,
            timestamp_ms = signal.timestamp_ms,
            strategy = ?signal.strategy,
            "Received trade signal"
        );

        let report = executor.execute(signal).await;
        let payload = serde_json::to_string(&report)?;
        publisher
            .add_json(&executor.config.execution_reports_stream, &payload)
            .await?;
        executor.store.record_execution_report(&report).await?;
        consumer.ack(&message.id).await?;
    }
}

struct OrderExecutor {
    config: Config,
    clob: Option<AuthenticatedClob>,
    risk: RiskService,
    store: StateStore,
    metrics: Metrics,
    order_tracker: OrderTracker,
    control_store: KeyValueStore,
}

impl OrderExecutor {
    async fn new(config: Config, order_tracker: OrderTracker) -> Result<Self> {
        let store = StateStore::connect(config.database_url.as_deref()).await?;
        let control_store = KeyValueStore::new(&config.redis_url).await?;
        let clob = match config.execution_mode {
            ExecutionMode::DryRun => None,
            ExecutionMode::Live => Some(AuthenticatedClob::from_config(&config).await?),
        };

        Ok(Self {
            config,
            clob,
            risk: RiskService::new(),
            store,
            metrics: Metrics::default(),
            order_tracker,
            control_store,
        })
    }

    async fn execute(&mut self, signal: TradeSignal) -> ExecutionReport {
        self.metrics.signal_received();
        if let Err(err) = self.store.record_signal(&signal).await {
            error!(
                signal_id = %signal.signal_id,
                error = %err,
                "Failed to persist trade signal"
            );
        }

        match self.try_execute(&signal).await {
            Ok(report) => report,
            Err(err) => {
                self.metrics.signal_rejected();
                ExecutionReport {
                    signal_id: signal.signal_id,
                    order_id: String::new(),
                    status: ExecutionStatus::Error,
                    filled_price: None,
                    filled_size: None,
                    error: Some(err.to_string()),
                    timestamp_ms: now_ms(),
                }
            }
        }
    }

    async fn try_execute(&mut self, signal: &TradeSignal) -> Result<ExecutionReport> {
        let mut effective_config = self.config.clone();
        effective_config.kill_switch = effective_config.kill_switch
            || self
                .control_store
                .get_bool(&effective_config.operator_kill_switch_key)
                .await?;
        self.risk.validate(signal, &effective_config)?;

        if self.config.execution_mode == ExecutionMode::DryRun {
            self.risk.record_accepted_signal(signal);
            let order_id = format!("dry-run-{}", signal.signal_id);
            self.order_tracker
                .track_submitted(signal, &order_id, now_ms())
                .await;
            self.record_order_submission(signal, &order_id).await;
            return Ok(ExecutionReport {
                signal_id: signal.signal_id.clone(),
                order_id,
                status: ExecutionStatus::Delayed,
                filled_price: None,
                filled_size: None,
                error: None,
                timestamp_ms: now_ms(),
            });
        }

        let clob = self
            .clob
            .as_ref()
            .context("CLOB client is not initialized")?;
        let token_id = U256::from_str(&signal.asset_id)
            .with_context(|| format!("invalid asset_id {}", signal.asset_id))?;
        let price = decimal_from_f64(signal.price)?;
        let size = decimal_from_f64(signal.size)?;
        let side = match signal.side {
            Side::Buy => ClobSide::Buy,
            Side::Sell => ClobSide::Sell,
        };

        let order = clob
            .client
            .limit_order()
            .token_id(token_id)
            .order_type(OrderType::GTC)
            .price(price)
            .size(size)
            .side(side)
            .build()
            .await?;
        let signed_order = clob.client.sign(&clob.signer, order).await?;
        let response = clob.client.post_order(signed_order).await?;
        if !response.success {
            self.metrics.clob_error();
        } else {
            self.metrics.order_submitted();
            self.risk.record_accepted_signal(signal);
            self.order_tracker
                .track_submitted(signal, &response.order_id, now_ms())
                .await;
            self.record_order_submission(signal, &response.order_id)
                .await;
        }

        Ok(ExecutionReport {
            signal_id: signal.signal_id.clone(),
            order_id: response.order_id,
            status: map_order_status(response.status),
            filled_price: None,
            filled_size: None,
            error: response.error_msg,
            timestamp_ms: now_ms(),
        })
    }

    async fn record_order_submission(&self, signal: &TradeSignal, order_id: &str) {
        if let Err(err) = self.store.record_order_submission(signal, order_id).await {
            error!(
                signal_id = %signal.signal_id,
                order_id = %order_id,
                error = %err,
                "Failed to persist submitted order"
            );
        }
    }
}

fn decimal_from_f64(value: f64) -> Result<Decimal> {
    if !value.is_finite() {
        bail!("numeric value must be finite");
    }
    Decimal::from_str(&format!("{value:.6}")).context("invalid decimal value")
}

fn map_order_status(status: OrderStatusType) -> ExecutionStatus {
    match status {
        OrderStatusType::Matched => ExecutionStatus::Matched,
        OrderStatusType::Delayed => ExecutionStatus::Delayed,
        OrderStatusType::Canceled => ExecutionStatus::Cancelled,
        OrderStatusType::Live | OrderStatusType::Unmatched | OrderStatusType::Unknown(_) => {
            ExecutionStatus::Unmatched
        }
        _ => ExecutionStatus::Unmatched,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX_EPOCH")
        .as_millis() as u64
}
