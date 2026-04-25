use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info, warn};

use crate::clob_client::AuthenticatedClob;
use crate::config::{Config, ExecutionMode};
use crate::executor::{ExecutionReport, ExecutionStatus};
use crate::reconciliation::OrderTracker;
use crate::redis_client::{StreamConsumer, StreamProducer};
use crate::state_store::StateStore;

#[derive(Debug, Deserialize)]
struct OperatorCommand {
    #[serde(rename = "type")]
    command_type: String,
    command_id: Option<String>,
    reason: Option<String>,
    operator: Option<String>,
    timestamp_ms: Option<u64>,
    confirm: Option<bool>,
    confirmation_phrase: Option<String>,
    scope: Option<String>,
}

pub async fn run(
    mut publisher: StreamProducer,
    mut consumer: StreamConsumer,
    config: Config,
    order_tracker: OrderTracker,
) -> Result<()> {
    info!("Starting operator control consumer...");
    let store = StateStore::connect(config.database_url.as_deref()).await?;
    let clob = match config.execution_mode {
        ExecutionMode::DryRun => None,
        ExecutionMode::Live => Some(AuthenticatedClob::from_config(&config).await?),
    };

    loop {
        let message = consumer.next_message().await?;
        let command: OperatorCommand = match serde_json::from_str(&message.payload) {
            Ok(command) => command,
            Err(err) => {
                error!(error = %err, raw = %message.payload, "Invalid operator command JSON");
                consumer.ack(&message.id).await?;
                continue;
            }
        };

        let command_id = command
            .command_id
            .clone()
            .unwrap_or_else(|| format!("legacy-{}", message.id));
        let result = match command.command_type.as_str() {
            "cancel_all" => {
                handle_cancel_all(
                    &mut publisher,
                    &store,
                    &config,
                    clob.as_ref(),
                    &order_tracker,
                    &command,
                )
                .await
            }
            "cancel_bot_open" => {
                handle_cancel_bot_open(
                    &mut publisher,
                    &store,
                    &config,
                    clob.as_ref(),
                    &order_tracker,
                    &command,
                )
                .await
            }
            _ => {
                consumer.ack(&message.id).await?;
                continue;
            }
        };
        if let Err(err) = result {
            error!(
                command_id = %command_id,
                error = %err,
                "Operator cancel_all failed"
            );
            publish_control_result(
                &mut publisher,
                &config.operator_results_stream,
                &command,
                &json!({
                    "type": format!("{}_result", command.command_type),
                    "command_id": command_id,
                    "command_type": command.command_type,
                    "status": "ERROR",
                    "error": err.to_string(),
                    "timestamp_ms": now_ms()
                }),
            )
            .await?;
        }
        consumer.ack(&message.id).await?;
    }
}

async fn handle_cancel_all(
    publisher: &mut StreamProducer,
    store: &StateStore,
    config: &Config,
    clob: Option<&AuthenticatedClob>,
    order_tracker: &OrderTracker,
    command: &OperatorCommand,
) -> Result<()> {
    let command_id = command
        .command_id
        .clone()
        .context("cancel_all command missing command_id")?;
    if command.confirm != Some(true)
        || command.confirmation_phrase.as_deref() != Some("CANCEL ALL OPEN ORDERS")
        || command.scope.as_deref() != Some("account")
    {
        publish_control_result(
            publisher,
            &config.operator_results_stream,
            command,
            &json!({
                "type": "cancel_all_result",
                "command_id": command_id,
                "command_type": "cancel_all",
                "status": "REJECTED",
                "error": "cancel_all requires confirm=true, scope=account, and confirmation_phrase",
                "timestamp_ms": now_ms()
            }),
        )
        .await?;
        return Ok(());
    }
    match config.execution_mode {
        ExecutionMode::DryRun => {
            let tracked = order_tracker.cancel_all_tracked().await;
            let canceled_count = tracked.len();
            for order in tracked {
                let report = ExecutionReport {
                    signal_id: order.signal_id,
                    order_id: order.order_id,
                    status: ExecutionStatus::Cancelled,
                    filled_price: None,
                    filled_size: None,
                    error: None,
                    timestamp_ms: now_ms(),
                };
                publish_execution_report(
                    publisher,
                    store,
                    &config.execution_reports_stream,
                    &report,
                )
                .await?;
            }
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_all_result",
                    "command_id": command_id,
                    "command_type": "cancel_all",
                    "status": "DRY_RUN",
                    "canceled_count": canceled_count,
                    "not_canceled": {},
                    "timestamp_ms": now_ms()
                }),
            )
            .await?;
        }
        ExecutionMode::Live => {
            let clob = clob.context("authenticated CLOB client is not initialized")?;
            let response = clob.client.cancel_all_orders().await?;
            let open_after = clob.client.orders(&OrdersRequest::default(), None).await?;
            let open_after_ids: Vec<String> =
                open_after.data.into_iter().map(|order| order.id).collect();
            let divergences: Vec<String> = response
                .canceled
                .iter()
                .filter(|order_id| open_after_ids.contains(order_id))
                .cloned()
                .collect();
            for order_id in &response.canceled {
                if let Some(order) = order_tracker.get(order_id).await {
                    let report = ExecutionReport {
                        signal_id: order.signal_id,
                        order_id: order.order_id,
                        status: ExecutionStatus::Cancelled,
                        filled_price: None,
                        filled_size: None,
                        error: None,
                        timestamp_ms: now_ms(),
                    };
                    publish_execution_report(
                        publisher,
                        store,
                        &config.execution_reports_stream,
                        &report,
                    )
                    .await?;
                    store
                        .record_cancel_request(
                            &command_id,
                            order_id,
                            "CONFIRMED",
                            &json!({"source": "cancel_all", "confirmed_by": "clob_open_orders"}),
                        )
                        .await?;
                } else {
                    warn!(
                        order_id = %order_id,
                        "CLOB cancel_all canceled an order not tracked by this runtime"
                    );
                    store
                        .record_cancel_request(
                            &command_id,
                            order_id,
                            "UNTRACKED",
                            &json!({"source": "cancel_all"}),
                        )
                        .await?;
                }
            }
            for (order_id, reason) in &response.not_canceled {
                store
                    .record_cancel_request(
                        &command_id,
                        order_id,
                        "FAILED",
                        &json!({"source": "cancel_all", "reason": reason}),
                    )
                    .await?;
            }
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_all_result",
                    "command_id": command_id,
                    "command_type": "cancel_all",
                    "status": if divergences.is_empty() { "CONFIRMED" } else { "DIVERGED" },
                    "canceled": response.canceled,
                    "not_canceled": response.not_canceled,
                    "divergences": divergences,
                    "timestamp_ms": now_ms()
                }),
            )
            .await?;
        }
    }
    Ok(())
}

async fn handle_cancel_bot_open(
    publisher: &mut StreamProducer,
    store: &StateStore,
    config: &Config,
    clob: Option<&AuthenticatedClob>,
    order_tracker: &OrderTracker,
    command: &OperatorCommand,
) -> Result<()> {
    let command_id = command
        .command_id
        .clone()
        .context("cancel_bot_open command missing command_id")?;
    let order_ids = bot_open_order_ids(store, order_tracker).await?;
    if order_ids.is_empty() {
        publish_control_result(
            publisher,
            &config.operator_results_stream,
            command,
            &json!({
                "type": "cancel_bot_open_result",
                "command_id": command_id,
                "command_type": "cancel_bot_open",
                "status": "NO_OPEN_ORDERS",
                "canceled": [],
                "not_canceled": {},
                "timestamp_ms": now_ms()
            }),
        )
        .await?;
        return Ok(());
    }

    match config.execution_mode {
        ExecutionMode::DryRun => {
            let tracked = order_tracker.remove_orders(&order_ids).await;
            for order in tracked {
                publish_cancelled_report(
                    publisher,
                    store,
                    &config.execution_reports_stream,
                    order.signal_id,
                    order.order_id,
                )
                .await?;
            }
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_bot_open_result",
                    "command_id": command_id,
                    "command_type": "cancel_bot_open",
                    "status": "DRY_RUN",
                    "canceled": order_ids,
                    "not_canceled": {},
                    "timestamp_ms": now_ms()
                }),
            )
            .await?;
        }
        ExecutionMode::Live => {
            let clob = clob.context("authenticated CLOB client is not initialized")?;
            let refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
            let response = clob.client.cancel_orders(&refs).await?;
            let open_after = clob.client.orders(&OrdersRequest::default(), None).await?;
            let open_after_ids: Vec<String> =
                open_after.data.into_iter().map(|order| order.id).collect();
            let divergences: Vec<String> = response
                .canceled
                .iter()
                .filter(|order_id| open_after_ids.contains(order_id))
                .cloned()
                .collect();
            for order_id in &response.canceled {
                if let Some(order) = resolve_bot_order(store, order_tracker, order_id).await? {
                    publish_cancelled_report(
                        publisher,
                        store,
                        &config.execution_reports_stream,
                        order.signal_id,
                        order.order_id,
                    )
                    .await?;
                    store
                        .record_cancel_request(
                            &command_id,
                            order_id,
                            "CONFIRMED",
                            &json!({"source": "cancel_bot_open", "confirmed_by": "clob_open_orders"}),
                        )
                        .await?;
                }
            }
            for (order_id, reason) in &response.not_canceled {
                store
                    .record_cancel_request(
                        &command_id,
                        order_id,
                        "FAILED",
                        &json!({"source": "cancel_bot_open", "reason": reason}),
                    )
                    .await?;
            }
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_bot_open_result",
                    "command_id": command_id,
                    "command_type": "cancel_bot_open",
                    "status": if divergences.is_empty() { "CONFIRMED" } else { "DIVERGED" },
                    "attempted": order_ids,
                    "canceled": response.canceled,
                    "not_canceled": response.not_canceled,
                    "divergences": divergences,
                    "timestamp_ms": now_ms()
                }),
            )
            .await?;
        }
    }
    Ok(())
}

async fn bot_open_order_ids(
    store: &StateStore,
    order_tracker: &OrderTracker,
) -> Result<Vec<String>> {
    let mut order_ids = order_tracker.open_order_ids().await;
    for order in store.open_bot_orders().await? {
        if !order_ids.contains(&order.order_id) {
            order_ids.push(order.order_id);
        }
    }
    Ok(order_ids)
}

async fn resolve_bot_order(
    store: &StateStore,
    order_tracker: &OrderTracker,
    order_id: &str,
) -> Result<Option<crate::reconciliation::TrackedOrder>> {
    if let Some(order) = order_tracker.get(order_id).await {
        return Ok(Some(order));
    }
    let Some(order) = store.find_order(order_id).await? else {
        return Ok(None);
    };
    Ok(Some(
        crate::reconciliation::TrackedOrder::from_stored_order(order_id, order),
    ))
}

async fn publish_cancelled_report(
    publisher: &mut StreamProducer,
    store: &StateStore,
    stream: &str,
    signal_id: String,
    order_id: String,
) -> Result<()> {
    let report = ExecutionReport {
        signal_id,
        order_id,
        status: ExecutionStatus::Cancelled,
        filled_price: None,
        filled_size: None,
        error: None,
        timestamp_ms: now_ms(),
    };
    publish_execution_report(publisher, store, stream, &report).await
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

async fn publish_control_result(
    publisher: &mut StreamProducer,
    stream: &str,
    command: &OperatorCommand,
    result: &serde_json::Value,
) -> Result<()> {
    info!(
        command_id = ?command.command_id,
        command_type = %command.command_type,
        reason = ?command.reason,
        operator = ?command.operator,
        timestamp_ms = ?command.timestamp_ms,
        result = %result,
        "Operator command result"
    );
    publisher.add_json(stream, &result.to_string()).await?;
    Ok(())
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

    #[test]
    fn cancel_all_command_deserializes() {
        let command: OperatorCommand = serde_json::from_str(
            r#"{
                "type": "cancel_all",
                "command_id": "command-1",
                "reason": "risk off",
                "operator": "operator-1",
                "timestamp_ms": 123,
                "confirm": true,
                "confirmation_phrase": "CANCEL ALL OPEN ORDERS",
                "scope": "account"
            }"#,
        )
        .unwrap();

        assert_eq!(command.command_type, "cancel_all");
        assert_eq!(command.command_id.as_deref(), Some("command-1"));
        assert_eq!(command.reason.as_deref(), Some("risk off"));
        assert_eq!(command.confirm, Some(true));
        assert_eq!(
            command.confirmation_phrase.as_deref(),
            Some("CANCEL ALL OPEN ORDERS")
        );
    }

    #[test]
    fn cancel_bot_open_command_deserializes() {
        let command: OperatorCommand = serde_json::from_str(
            r#"{
                "type": "cancel_bot_open",
                "command_id": "command-2",
                "reason": "rebalance",
                "operator": "operator-1",
                "timestamp_ms": 123
            }"#,
        )
        .unwrap();

        assert_eq!(command.command_type, "cancel_bot_open");
        assert_eq!(command.command_id.as_deref(), Some("command-2"));
    }
}
