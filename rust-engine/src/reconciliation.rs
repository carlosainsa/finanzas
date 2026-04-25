use std::collections::{HashMap, HashSet};
use std::fmt::Display;
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

use crate::clob_client::AuthenticatedClob;
use crate::config::{Config, ExecutionMode};
use crate::executor::{ExecutionReport, ExecutionStatus, Side, TradeSignal};
use crate::redis_client::StreamProducer;
use crate::state_store::{StateStore, StoredOrder, TradeLifecycleUpdate};

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
    side: Option<Side>,
    limit_price: Option<f64>,
    requested_size: f64,
    filled_size: f64,
    remaining_size: f64,
    seen_trade_ids: HashSet<String>,
    submitted_at_ms: u64,
    dry_run_reported: bool,
}

impl TrackedOrder {
    pub(crate) fn from_stored_order(_order_id: &str, order: StoredOrder) -> Self {
        let requested_size = order
            .requested_size
            .unwrap_or_else(|| order.filled_size + order.remaining_size.unwrap_or(0.0));
        let remaining_size = order
            .remaining_size
            .unwrap_or_else(|| (requested_size - order.filled_size).max(0.0));
        Self {
            signal_id: order.signal_id,
            order_id: order.order_id,
            market_id: order.market_id,
            asset_id: order.asset_id,
            side: order.side.as_deref().and_then(parse_signal_side),
            limit_price: order.limit_price,
            requested_size,
            filled_size: order.filled_size,
            remaining_size,
            seen_trade_ids: HashSet::new(),
            submitted_at_ms: now_ms(),
            dry_run_reported: true,
        }
    }

    fn fill_state_after(&self, fill_size: f64) -> OrderFillState {
        let filled_size = (self.filled_size + fill_size).min(self.requested_size);
        let remaining_size = (self.requested_size - filled_size).max(0.0);
        OrderFillState {
            filled_size,
            remaining_size,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OrderFillState {
    filled_size: f64,
    remaining_size: f64,
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
            side: Some(signal.side),
            limit_price: Some(signal.price),
            requested_size: signal.size,
            filled_size: 0.0,
            remaining_size: signal.size,
            seen_trade_ids: HashSet::new(),
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

    pub(crate) async fn open_order_ids(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }

    pub(crate) async fn remove_orders(&self, order_ids: &[String]) -> Vec<TrackedOrder> {
        let mut inner = self.inner.lock().await;
        order_ids
            .iter()
            .filter_map(|order_id| inner.remove(order_id))
            .collect()
    }

    pub(crate) async fn remove_order(&self, order_id: &str) -> Option<TrackedOrder> {
        self.inner.lock().await.remove(order_id)
    }

    async fn apply_trade_fill(
        &self,
        order_id: &str,
        trade_id: &str,
        fill_size: f64,
    ) -> Option<OrderFillState> {
        let mut inner = self.inner.lock().await;
        let order = inner.get_mut(order_id)?;
        if order.seen_trade_ids.insert(trade_id.to_owned()) {
            order.filled_size = (order.filled_size + fill_size).min(order.requested_size);
            order.remaining_size = (order.requested_size - order.filled_size).max(0.0);
        }
        let state = OrderFillState {
            filled_size: order.filled_size,
            remaining_size: order.remaining_size,
        };
        if state.remaining_size <= f64::EPSILON {
            inner.remove(order_id);
        }
        Some(state)
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

    let clob = AuthenticatedClob::from_config(&config).await?;

    loop {
        match run_live_session(
            &mut publisher,
            &store,
            &config,
            &auth,
            &order_tracker,
            &clob,
        )
        .await
        {
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
                cumulative_filled_size: None,
                remaining_size: Some(order.remaining_size),
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
    clob: &AuthenticatedClob,
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
                        match reconcile_user_message(text, order_tracker, store, Some(clob)).await {
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
    clob: Option<&AuthenticatedClob>,
) -> Result<Vec<ExecutionReport>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    match value {
        serde_json::Value::Array(items) => {
            let mut reports = Vec::new();
            for item in items {
                if let Some(report) = reconcile_user_value(item, order_tracker, store, clob).await?
                {
                    reports.push(report);
                }
            }
            Ok(reports)
        }
        value => Ok(reconcile_user_value(value, order_tracker, store, clob)
            .await?
            .into_iter()
            .collect()),
    }
}

async fn reconcile_user_value(
    value: serde_json::Value,
    order_tracker: &OrderTracker,
    store: &StateStore,
    clob: Option<&AuthenticatedClob>,
) -> Result<Option<ExecutionReport>> {
    match value.get("event_type").and_then(|event| event.as_str()) {
        Some("order") => {
            let event: UserOrderEvent = serde_json::from_value(value.clone())?;
            reconcile_order_event(event, value, order_tracker, store).await
        }
        Some("trade") => {
            let event: UserTradeEvent = serde_json::from_value(value.clone())?;
            reconcile_trade_event(event, value, order_tracker, store, clob).await
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
    if status == ExecutionStatus::Cancelled {
        order_tracker.remove_order(&event.id).await;
        store
            .confirm_cancel_requests_for_order(
                &event.id,
                &json!({"confirmed_by": "user_ws", "event": payload}),
            )
            .await?;
    }

    Ok(Some(ExecutionReport {
        signal_id: order.signal_id,
        order_id: event.id,
        status,
        filled_price: Some(parse_decimal(&event.price)?),
        filled_size: Some(parse_decimal(&event.size_matched)?),
        cumulative_filled_size: Some(parse_decimal(&event.size_matched)?),
        remaining_size: Some(if status == ExecutionStatus::Cancelled {
            0.0
        } else {
            (order.requested_size - parse_decimal(&event.size_matched)?).max(0.0)
        }),
        error: None,
        timestamp_ms: seconds_to_ms(&event.timestamp)?,
    }))
}

async fn reconcile_trade_event(
    event: UserTradeEvent,
    payload: serde_json::Value,
    order_tracker: &OrderTracker,
    store: &StateStore,
    clob: Option<&AuthenticatedClob>,
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
    let fill_price = parse_decimal(&event.price)?;
    let fill_size = parse_trade_size(&event, &order.order_id)?;
    let divergence = fill_divergence(&order, fill_price, fill_size);
    let fill_state = order_tracker
        .apply_trade_fill(&order_id, &event.id, fill_size)
        .await
        .unwrap_or_else(|| order.fill_state_after(fill_size));
    let status = map_trade_status(&event.status, fill_state.remaining_size);
    let poll_divergence = match clob {
        Some(clob) => poll_order_fill_divergence(clob, &order_id, fill_state.filled_size).await,
        None => None,
    };
    store
        .record_trade_lifecycle(TradeLifecycleUpdate {
            trade_id: &event.id,
            order_id: &order_id,
            signal_id: &order.signal_id,
            status: execution_status_name(status),
            payload: &payload,
            fill_price: Some(fill_price),
            fill_size: Some(fill_size),
        })
        .await?;

    Ok(Some(ExecutionReport {
        signal_id: order.signal_id,
        order_id,
        status,
        filled_price: Some(fill_price),
        filled_size: Some(fill_size),
        cumulative_filled_size: Some(fill_state.filled_size),
        remaining_size: Some(fill_state.remaining_size),
        error: divergence
            .or(poll_divergence)
            .or_else(|| trade_error(&event.status)),
        timestamp_ms: seconds_to_ms(&event.timestamp)?,
    }))
}

async fn poll_order_fill_divergence(
    clob: &AuthenticatedClob,
    order_id: &str,
    expected_filled_size: f64,
) -> Option<String> {
    match clob.client.order(order_id).await {
        Ok(order) => {
            let Some(size_matched) = display_to_f64(order.size_matched) else {
                return Some("fill divergence: CLOB size_matched is not numeric".to_owned());
            };
            polled_fill_divergence(size_matched, expected_filled_size)
        }
        Err(err) => {
            warn!(
                order_id = %order_id,
                error = %err,
                "Could not poll CLOB order after user trade event"
            );
            None
        }
    }
}

fn polled_fill_divergence(polled_size_matched: f64, expected_filled_size: f64) -> Option<String> {
    const EPSILON: f64 = 1e-9;
    if (polled_size_matched - expected_filled_size).abs() > EPSILON {
        return Some(format!(
            "fill divergence: CLOB size_matched {polled_size_matched} differs from user_ws cumulative_filled_size {expected_filled_size}"
        ));
    }
    None
}

fn display_to_f64(value: impl Display) -> Option<f64> {
    value
        .to_string()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
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

fn map_trade_status(status: &str, remaining_size: f64) -> ExecutionStatus {
    match status {
        "MATCHED" | "MINED" | "CONFIRMED" if remaining_size <= f64::EPSILON => {
            ExecutionStatus::Matched
        }
        "MATCHED" | "MINED" | "CONFIRMED" => ExecutionStatus::Partial,
        "FAILED" => ExecutionStatus::Error,
        _ => ExecutionStatus::Delayed,
    }
}

fn trade_error(status: &str) -> Option<String> {
    (status == "FAILED").then(|| "trade failed".to_owned())
}

fn fill_divergence(order: &TrackedOrder, fill_price: f64, fill_size: f64) -> Option<String> {
    const EPSILON: f64 = 1e-9;
    if fill_size - order.remaining_size > EPSILON {
        return Some(format!(
            "fill divergence: fill_size {fill_size} exceeds remaining_size {}",
            order.remaining_size
        ));
    }
    match (order.side, order.limit_price) {
        (Some(Side::Buy), Some(limit_price)) if fill_price - limit_price > EPSILON => {
            Some(format!(
                "fill divergence: buy fill_price {fill_price} exceeds limit_price {limit_price}"
            ))
        }
        (Some(Side::Sell), Some(limit_price)) if limit_price - fill_price > EPSILON => {
            Some(format!(
                "fill divergence: sell fill_price {fill_price} is below limit_price {limit_price}"
            ))
        }
        _ => None,
    }
}

fn parse_signal_side(value: &str) -> Option<Side> {
    match value {
        "BUY" | "Buy" | "buy" => Some(Side::Buy),
        "SELL" | "Sell" | "sell" => Some(Side::Sell),
        _ => None,
    }
}

fn execution_status_name(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Matched => "MATCHED",
        ExecutionStatus::Partial => "PARTIAL",
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
            source_timestamp_ms: None,
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
            None,
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
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Cancelled);
        assert!(tracker.get("order-1").await.is_none());
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
            None,
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
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Partial);
        assert_eq!(report.filled_size, Some(4.0));
        assert_eq!(report.cumulative_filled_size, Some(4.0));
        assert_eq!(report.remaining_size, Some(6.0));
    }

    #[tokio::test]
    async fn partial_fills_accumulate_until_terminal_fill() {
        let tracker = tracker().await;
        let store = store();
        let first = reconcile_user_message(
            r#"{
                "asset_id": "asset-1",
                "event_type": "trade",
                "id": "trade-1",
                "maker_orders": [],
                "market": "0xmarket",
                "price": "0.57",
                "side": "BUY",
                "size": "4",
                "status": "MATCHED",
                "taker_order_id": "order-1",
                "timestamp": "1672290701",
                "type": "TRADE"
            }"#,
            &tracker,
            &store,
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        let second = reconcile_user_message(
            r#"{
                "asset_id": "asset-1",
                "event_type": "trade",
                "id": "trade-2",
                "maker_orders": [],
                "market": "0xmarket",
                "price": "0.57",
                "side": "BUY",
                "size": "6",
                "status": "MATCHED",
                "taker_order_id": "order-1",
                "timestamp": "1672290702",
                "type": "TRADE"
            }"#,
            &tracker,
            &store,
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(first.status, ExecutionStatus::Partial);
        assert_eq!(first.cumulative_filled_size, Some(4.0));
        assert_eq!(first.remaining_size, Some(6.0));
        assert_eq!(second.status, ExecutionStatus::Matched);
        assert_eq!(second.cumulative_filled_size, Some(10.0));
        assert_eq!(second.remaining_size, Some(0.0));
        assert!(tracker.get("order-1").await.is_none());
    }

    #[tokio::test]
    async fn duplicate_trade_id_does_not_double_count_fill() {
        let tracker = tracker().await;
        let store = store();
        let payload = r#"{
            "asset_id": "asset-1",
            "event_type": "trade",
            "id": "trade-1",
            "maker_orders": [],
            "market": "0xmarket",
            "price": "0.57",
            "side": "BUY",
            "size": "4",
            "status": "MATCHED",
            "taker_order_id": "order-1",
            "timestamp": "1672290701",
            "type": "TRADE"
        }"#;
        let first = reconcile_user_message(payload, &tracker, &store, None)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let second = reconcile_user_message(payload, &tracker, &store, None)
            .await
            .unwrap()
            .pop()
            .unwrap();

        assert_eq!(first.cumulative_filled_size, Some(4.0));
        assert_eq!(second.cumulative_filled_size, Some(4.0));
        assert_eq!(second.remaining_size, Some(6.0));
    }

    #[tokio::test]
    async fn trade_price_above_buy_limit_marks_divergence() {
        let tracker = tracker().await;
        let store = store();
        let report = reconcile_user_message(
            r#"{
                "asset_id": "asset-1",
                "event_type": "trade",
                "id": "trade-1",
                "maker_orders": [],
                "market": "0xmarket",
                "price": "0.58",
                "side": "BUY",
                "size": "2",
                "status": "MATCHED",
                "taker_order_id": "order-1",
                "timestamp": "1672290701",
                "type": "TRADE"
            }"#,
            &tracker,
            &store,
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Partial);
        assert!(report
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("fill_price"));
    }

    #[tokio::test]
    async fn trade_size_above_remaining_marks_divergence() {
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
                "size": "11",
                "status": "MATCHED",
                "taker_order_id": "order-1",
                "timestamp": "1672290701",
                "type": "TRADE"
            }"#,
            &tracker,
            &store,
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(report.status, ExecutionStatus::Matched);
        assert!(report
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("remaining_size"));
    }

    #[test]
    fn polled_fill_size_mismatch_marks_divergence() {
        let divergence = polled_fill_divergence(3.0, 4.0);

        assert!(divergence
            .as_deref()
            .unwrap_or_default()
            .contains("size_matched"));
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
            None,
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
            None,
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
