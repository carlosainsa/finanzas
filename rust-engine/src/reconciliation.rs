use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::config::{Config, ExecutionMode};
use crate::executor::{ExecutionReport, ExecutionStatus, TradeSignal};
use crate::redis_client::StreamProducer;
use crate::state_store::{StateStore, StoredOrder};

#[derive(Clone, Default)]
pub struct OrderTracker {
    inner: std::sync::Arc<Mutex<HashMap<String, TrackedOrder>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TrackedOrder {
    pub(crate) signal_id: String,
    pub(crate) order_id: String,
    market_id: String,
    asset_id: String,
    submitted_at_ms: u64,
    dry_run_reported: bool,
}

impl TrackedOrder {
    fn from_stored_order(order_id: &str, order: StoredOrder) -> Self {
        Self {
            signal_id: order.signal_id,
            order_id: order_id.to_owned(),
            market_id: order.market_id,
            asset_id: order.asset_id,
            submitted_at_ms: now_ms(),
            dry_run_reported: true,
        }
    }
}

impl OrderTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn track_submitted(
        &self,
        signal: &TradeSignal,
        order_id: &str,
        submitted_at_ms: u64,
    ) {
        let order = TrackedOrder {
            signal_id: signal.signal_id.clone(),
            order_id: order_id.to_owned(),
            market_id: signal.market_id.clone(),
            asset_id: signal.asset_id.clone(),
            submitted_at_ms,
            dry_run_reported: false,
        };
        self.inner
            .lock()
            .await
            .insert(order.order_id.clone(), order);
    }

    pub(crate) async fn get(&self, order_id: &str) -> Option<TrackedOrder> {
        self.inner.lock().await.get(order_id).cloned()
    }

    pub(crate) async fn cancel_all_tracked(&self) -> Vec<TrackedOrder> {
        self.inner
            .lock()
            .await
            .drain()
            .map(|(_, order)| order)
            .collect()
    }

    async fn dry_run_due(&self, timeout_ms: u64, now_ms: u64) -> Vec<TrackedOrder> {
        let mut inner = self.inner.lock().await;
        inner
            .values_mut()
            .filter(|order| {
                !order.dry_run_reported
                    && now_ms.saturating_sub(order.submitted_at_ms) >= timeout_ms
            })
            .map(|order| {
                order.dry_run_reported = true;
                order.clone()
            })
            .collect()
    }
}

pub async fn run(
    mut publisher: StreamProducer,
    config: Config,
    order_tracker: OrderTracker,
) -> Result<()> {
    let store = StateStore::connect(config.database_url.as_deref()).await?;

    if config.execution_mode == ExecutionMode::DryRun {
        info!("Starting dry-run order reconciliation simulator");
        return run_dry_run_reconciliation(
            &mut publisher,
            &store,
            order_tracker,
            &config.execution_reports_stream,
            config.order_reconciliation_timeout_ms,
        )
        .await;
    }

    let auth = UserWsAuth::from_config(&config)
        .context("Polymarket user WebSocket credentials are required in live mode")?;
    if config.user_market_ids.is_empty() {
        anyhow::bail!("USER_MARKET_IDS is required for live user WebSocket reconciliation");
    }

    loop {
        match run_live_session(&mut publisher, &store, &config, &auth, &order_tracker).await {
            Ok(()) => warn!("Polymarket user WebSocket session ended; reconnecting"),
            Err(err) => {
                error!(error = %err, "Polymarket user WebSocket session failed; reconnecting")
            }
        }
        time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_dry_run_reconciliation(
    publisher: &mut StreamProducer,
    store: &StateStore,
    order_tracker: OrderTracker,
    execution_reports_stream: &str,
    timeout_ms: u64,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_millis(250));
    loop {
        interval.tick().await;
        for order in order_tracker.dry_run_due(timeout_ms, now_ms()).await {
            let report = ExecutionReport {
                signal_id: order.signal_id,
                order_id: order.order_id,
                status: ExecutionStatus::Unmatched,
                filled_price: None,
                filled_size: None,
                error: None,
                timestamp_ms: now_ms(),
            };
            publish_execution_report(publisher, store, execution_reports_stream, &report).await?;
        }
    }
}

async fn run_live_session(
    publisher: &mut StreamProducer,
    store: &StateStore,
    config: &Config,
    auth: &UserWsAuth,
    order_tracker: &OrderTracker,
) -> Result<()> {
    info!(
        "Connecting to Polymarket user WebSocket: {}",
        config.polymarket_user_ws_url
    );
    let (ws_stream, _) = connect_async(&config.polymarket_user_ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscription = json!({
        "auth": {
            "apiKey": auth.api_key,
            "secret": auth.secret,
            "passphrase": auth.passphrase,
        },
        "markets": config.user_market_ids,
        "type": "user",
    });
    write.send(Message::Text(subscription.to_string())).await?;
    info!("Subscribed to Polymarket user channel");

    let mut heartbeat = time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                write.send(Message::Text("PING".to_owned())).await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                match message {
                    Ok(message) if message.is_text() => {
                        let text = message.to_text()?;
                        match reconcile_user_message(text, order_tracker, store).await {
                            Ok(reports) => {
                                for report in reports {
                                    publish_execution_report(
                                        publisher,
                                        store,
                                        &config.execution_reports_stream,
                                        &report,
                                    )
                                    .await?;
                                }
                            }
                            Err(err) => error!(error = %err, raw = %text, "Invalid Polymarket user message"),
                        }
                    }
                    Ok(_) => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }
    }
}

async fn publish_execution_report(
    publisher: &mut StreamProducer,
    store: &StateStore,
    stream: &str,
    report: &ExecutionReport,
) -> Result<()> {
    let payload = serde_json::to_string(report)?;
    publisher.add_json(stream, &payload).await?;
    store.record_execution_report(report).await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct UserWsAuth {
    api_key: String,
    secret: String,
    passphrase: String,
}

impl UserWsAuth {
    fn from_config(config: &Config) -> Option<Self> {
        Some(Self {
            api_key: config.polymarket_api_key.clone()?,
            secret: config.polymarket_api_secret.clone()?,
            passphrase: config.polymarket_api_passphrase.clone()?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct UserOrderEvent {
    event_type: String,
    id: String,
    market: String,
    asset_id: String,
    price: String,
    size_matched: String,
    timestamp: String,
    #[serde(rename = "type")]
    update_type: String,
}

#[derive(Debug, Deserialize)]
struct UserTradeEvent {
    event_type: String,
    id: String,
    price: String,
    size: String,
    status: String,
    timestamp: String,
    taker_order_id: String,
    maker_orders: Vec<UserMakerOrder>,
}

#[derive(Debug, Deserialize)]
struct UserMakerOrder {
    order_id: String,
    matched_amount: String,
}

async fn reconcile_user_message(
    raw: &str,
    order_tracker: &OrderTracker,
    store: &StateStore,
) -> Result<Vec<ExecutionReport>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    match value {
        serde_json::Value::Array(items) => {
            let mut reports = Vec::new();
            for item in items {
                if let Some(report) = reconcile_user_value(item, order_tracker, store).await? {
                    reports.push(report);
                }
            }
            Ok(reports)
        }
        value => Ok(reconcile_user_value(value, order_tracker, store)
            .await?
            .into_iter()
            .collect()),
    }
}

async fn reconcile_user_value(
    value: serde_json::Value,
    order_tracker: &OrderTracker,
    store: &StateStore,
) -> Result<Option<ExecutionReport>> {
    match value.get("event_type").and_then(|event| event.as_str()) {
        Some("order") => {
            let event: UserOrderEvent = serde_json::from_value(value.clone())?;
            reconcile_order_event(event, value, order_tracker, store).await
        }
        Some("trade") => {
            let event: UserTradeEvent = serde_json::from_value(value.clone())?;
            reconcile_trade_event(event, value, order_tracker, store).await
        }
        _ => Ok(None),
    }
}

async fn reconcile_order_event(
    event: UserOrderEvent,
    payload: serde_json::Value,
    order_tracker: &OrderTracker,
    store: &StateStore,
) -> Result<Option<ExecutionReport>> {
    if event.event_type != "order" {
        return Ok(None);
    }

    let Some(order) = resolve_order(&event.id, order_tracker, store).await? else {
        warn!(order_id = %event.id, "Ignoring user order event without tracked signal");
        return Ok(None);
    };
    if order.market_id != event.market || order.asset_id != event.asset_id {
        warn!(
            order_id = %event.id,
            tracked_market_id = %order.market_id,
            event_market_id = %event.market,
            tracked_asset_id = %order.asset_id,
            event_asset_id = %event.asset_id,
            "Ignoring user order event with mismatched tracked order metadata"
        );
        return Ok(None);
    }

    let status = match event.update_type.as_str() {
        "CANCELLATION" => ExecutionStatus::Cancelled,
        "PLACEMENT" | "UPDATE" => ExecutionStatus::Unmatched,
        _ => ExecutionStatus::Unmatched,
    };
    store
        .record_order_lifecycle(&event.id, execution_status_name(status), &payload)
        .await?;

    Ok(Some(ExecutionReport {
        signal_id: order.signal_id,
        order_id: event.id,
        status,
        filled_price: Some(parse_decimal(&event.price)?),
        filled_size: Some(parse_decimal(&event.size_matched)?),
        error: None,
        timestamp_ms: seconds_to_ms(&event.timestamp)?,
    }))
}

async fn reconcile_trade_event(
    event: UserTradeEvent,
    payload: serde_json::Value,
    order_tracker: &OrderTracker,
    store: &StateStore,
) -> Result<Option<ExecutionReport>> {
    if event.event_type != "trade" {
        return Ok(None);
    }

    let order_id = select_tracked_trade_order_id(&event, order_tracker, store).await?;
    let Some(order_id) = order_id else {
        warn!(trade_id = %event.id, "Ignoring user trade event without tracked order");
        return Ok(None);
    };
    let Some(order) = resolve_order(&order_id, order_tracker, store).await? else {
        return Ok(None);
    };
    let status = map_trade_status(&event.status);
    store
        .record_trade_lifecycle(
            &event.id,
            &order_id,
            &order.signal_id,
            execution_status_name(status),
            &payload,
        )
        .await?;

    Ok(Some(ExecutionReport {
        signal_id: order.signal_id,
        order_id,
        status,
        filled_price: Some(parse_decimal(&event.price)?),
        filled_size: Some(parse_trade_size(&event, &order.order_id)?),
        error: trade_error(&event.status),
        timestamp_ms: seconds_to_ms(&event.timestamp)?,
    }))
}

async fn select_tracked_trade_order_id(
    event: &UserTradeEvent,
    order_tracker: &OrderTracker,
    store: &StateStore,
) -> Result<Option<String>> {
    if resolve_order(&event.taker_order_id, order_tracker, store)
        .await?
        .is_some()
    {
        return Ok(Some(event.taker_order_id.clone()));
    }

    for maker_order in &event.maker_orders {
        if resolve_order(&maker_order.order_id, order_tracker, store)
            .await?
            .is_some()
        {
            return Ok(Some(maker_order.order_id.clone()));
        }
    }
    Ok(None)
}

async fn resolve_order(
    order_id: &str,
    order_tracker: &OrderTracker,
    store: &StateStore,
) -> Result<Option<TrackedOrder>> {
    if let Some(order) = order_tracker.get(order_id).await {
        return Ok(Some(order));
    }

    let Some(order) = store.find_order(order_id).await? else {
        return Ok(None);
    };

    Ok(Some(TrackedOrder::from_stored_order(order_id, order)))
}

fn parse_trade_size(event: &UserTradeEvent, order_id: &str) -> Result<f64> {
    if event.taker_order_id == order_id {
        return parse_decimal(&event.size);
    }

    event
        .maker_orders
        .iter()
        .find(|order| order.order_id == order_id)
        .map(|order| parse_decimal(&order.matched_amount))
        .unwrap_or_else(|| parse_decimal(&event.size))
}

fn map_trade_status(status: &str) -> ExecutionStatus {
    match status {
        "MATCHED" | "MINED" | "CONFIRMED" => ExecutionStatus::Matched,
        "FAILED" => ExecutionStatus::Error,
        _ => ExecutionStatus::Delayed,
    }
}

fn trade_error(status: &str) -> Option<String> {
    (status == "FAILED").then(|| "trade failed".to_owned())
}

fn execution_status_name(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Matched => "MATCHED",
        ExecutionStatus::Delayed => "DELAYED",
        ExecutionStatus::Unmatched => "UNMATCHED",
        ExecutionStatus::Cancelled => "CANCELLED",
        ExecutionStatus::Error => "ERROR",
    }
}

fn parse_decimal(value: &str) -> Result<f64> {
    let parsed: f64 = value.parse()?;
    if !parsed.is_finite() {
        anyhow::bail!("numeric value must be finite");
    }
    Ok(parsed)
}

fn seconds_to_ms(value: &str) -> Result<u64> {
    let timestamp: u64 = value.parse()?;
    Ok(if timestamp < 10_000_000_000 {
        timestamp * 1_000
    } else {
        timestamp
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX_EPOCH")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Side;

    fn signal() -> TradeSignal {
        TradeSignal {
            signal_id: "signal-1".to_owned(),
            market_id: "0xmarket".to_owned(),
            asset_id: "asset-1".to_owned(),
            side: Side::Buy,
            price: 0.57,
            size: 10.0,
            confidence: 0.9,
            timestamp_ms: now_ms(),
            strategy: Some("test".to_owned()),
        }
    }

    async fn tracker() -> OrderTracker {
        let tracker = OrderTracker::new();
        tracker
            .track_submitted(&signal(), "order-1", now_ms())
            .await;
        tracker
    }

    fn store() -> StateStore {
        StateStore::disabled()
    }

    #[tokio::test]
    async fn order_update_maps_to_unmatched_report() {
        let tracker = tracker().await;
        let store = store();
        let report = reconcile_user_message(
            r#"{
                "event_type": "order",
                "id": "order-1",
                "market": "0xmarket",
                "asset_id": "asset-1",
                "price": "0.57",
                "size_matched": "0",
                "timestamp": "1672290687",
                "type": "UPDATE"
            }"#,
            &tracker,
            &store,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.signal_id, "signal-1");
        assert_eq!(report.status, ExecutionStatus::Unmatched);
        assert_eq!(report.filled_size, Some(0.0));
    }

    #[tokio::test]
    async fn cancellation_maps_to_cancelled_report() {
        let tracker = tracker().await;
        let store = store();
        let report = reconcile_user_message(
            r#"{
                "event_type": "order",
                "id": "order-1",
                "market": "0xmarket",
                "asset_id": "asset-1",
                "price": "0.57",
                "size_matched": "0",
                "timestamp": "1672290687",
                "type": "CANCELLATION"
            }"#,
            &tracker,
            &store,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Cancelled);
    }

    #[tokio::test]
    async fn trade_matched_maps_to_filled_report() {
        let tracker = tracker().await;
        let store = store();
        let report = reconcile_user_message(
            r#"{
                "asset_id": "asset-1",
                "event_type": "trade",
                "id": "trade-1",
                "maker_orders": [],
                "market": "0xmarket",
                "price": "0.57",
                "side": "BUY",
                "size": "10",
                "status": "MATCHED",
                "taker_order_id": "order-1",
                "timestamp": "1672290701",
                "type": "TRADE"
            }"#,
            &tracker,
            &store,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Matched);
        assert_eq!(report.filled_price, Some(0.57));
        assert_eq!(report.filled_size, Some(10.0));
    }

    #[tokio::test]
    async fn maker_trade_uses_maker_matched_amount() {
        let tracker = tracker().await;
        let store = store();
        let report = reconcile_user_message(
            r#"{
                "asset_id": "asset-1",
                "event_type": "trade",
                "id": "trade-1",
                "maker_orders": [{
                    "asset_id": "asset-1",
                    "matched_amount": "4",
                    "order_id": "order-1",
                    "outcome": "YES",
                    "owner": "owner",
                    "price": "0.57"
                }],
                "market": "0xmarket",
                "price": "0.57",
                "side": "BUY",
                "size": "10",
                "status": "MATCHED",
                "taker_order_id": "other-order",
                "timestamp": "1672290701",
                "type": "TRADE"
            }"#,
            &tracker,
            &store,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Matched);
        assert_eq!(report.filled_size, Some(4.0));
    }

    #[tokio::test]
    async fn unknown_order_event_is_ignored() {
        let tracker = OrderTracker::new();
        let store = store();
        assert!(reconcile_user_message(
            r#"{
                "event_type": "order",
                "id": "order-1",
                "market": "0xmarket",
                "asset_id": "asset-1",
                "price": "0.57",
                "size_matched": "0",
                "timestamp": "1672290687",
                "type": "UPDATE"
            }"#,
            &tracker,
            &store,
        )
        .await
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn invalid_numeric_field_returns_error() {
        let tracker = tracker().await;
        let store = store();
        assert!(reconcile_user_message(
            r#"{
                "event_type": "order",
                "id": "order-1",
                "market": "0xmarket",
                "asset_id": "asset-1",
                "price": "not-a-number",
                "size_matched": "0",
                "timestamp": "1672290687",
                "type": "UPDATE"
            }"#,
            &tracker,
            &store,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn dry_run_due_marks_order_once() {
        let tracker = OrderTracker::new();
        tracker
            .track_submitted(&signal(), "dry-run-signal-1", 1)
            .await;

        assert_eq!(tracker.dry_run_due(1, 10).await.len(), 1);
        assert!(tracker.dry_run_due(1, 10).await.is_empty());
    }
}
