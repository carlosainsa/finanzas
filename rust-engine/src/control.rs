use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use serde::Deserialize;
use serde_json::json;
use tokio::time::{sleep, Duration};
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
                &store,
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
            store,
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
                    cumulative_filled_size: None,
                    remaining_size: Some(0.0),
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
                store,
            )
            .await?;
        }
        ExecutionMode::Live => {
            let clob = clob.context("authenticated CLOB client is not initialized")?;
            let response = clob.client.cancel_all_orders().await?;
            for order_id in &response.canceled {
                info!(
                    command_id = %command_id,
                    order_id = %order_id,
                    "CLOB cancel_all returned canceled order"
                );
                store
                    .record_cancel_request(
                        &command_id,
                        order_id,
                        "SENT",
                        &json!({"source": "cancel_all"}),
                    )
                    .await?;
            }
            for (order_id, reason) in &response.not_canceled {
                warn!(
                    command_id = %command_id,
                    order_id = %order_id,
                    reason = %reason,
                    "CLOB cancel_all did not cancel order"
                );
                store
                    .record_cancel_request(
                        &command_id,
                        order_id,
                        "FAILED",
                        &json!({"source": "cancel_all", "reason": reason}),
                    )
                    .await?;
            }
            let user_ws_statuses = wait_for_cancel_confirmations(
                store,
                &command_id,
                &response.canceled,
                config.cancel_confirmation_timeout_ms,
            )
            .await?;
            let open_after = clob.client.orders(&OrdersRequest::default(), None).await?;
            let open_after_ids: HashSet<String> =
                open_after.data.into_iter().map(|order| order.id).collect();
            let divergences = finalize_cancel_confirmations(CancelFinalizeContext {
                publisher,
                store,
                execution_reports_stream: &config.execution_reports_stream,
                order_tracker,
                command_id: &command_id,
                source: "cancel_all",
                canceled_order_ids: &response.canceled,
                user_ws_statuses: &user_ws_statuses,
                open_after_ids: &open_after_ids,
            })
            .await?;
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_all_result",
                    "command_id": command_id,
                    "command_type": "cancel_all",
                    "status": cancel_result_status(&divergences, &response.not_canceled),
                    "canceled": &response.canceled,
                    "not_canceled": &response.not_canceled,
                    "divergences": divergences,
                    "timestamp_ms": now_ms()
                }),
                store,
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
            store,
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
                store,
            )
            .await?;
        }
        ExecutionMode::Live => {
            let clob = clob.context("authenticated CLOB client is not initialized")?;
            let refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
            for order_id in &order_ids {
                info!(
                    command_id = %command_id,
                    order_id = %order_id,
                    "Sending bot-scoped cancel request"
                );
                store
                    .record_cancel_request(
                        &command_id,
                        order_id,
                        "SENT",
                        &json!({"source": "cancel_bot_open"}),
                    )
                    .await?;
            }
            let response = clob.client.cancel_orders(&refs).await?;
            for (order_id, reason) in &response.not_canceled {
                warn!(
                    command_id = %command_id,
                    order_id = %order_id,
                    reason = %reason,
                    "Bot-scoped cancel failed"
                );
                store
                    .record_cancel_request(
                        &command_id,
                        order_id,
                        "FAILED",
                        &json!({"source": "cancel_bot_open", "reason": reason}),
                    )
                    .await?;
            }
            let user_ws_statuses = wait_for_cancel_confirmations(
                store,
                &command_id,
                &response.canceled,
                config.cancel_confirmation_timeout_ms,
            )
            .await?;
            let open_after = clob.client.orders(&OrdersRequest::default(), None).await?;
            let open_after_ids: HashSet<String> =
                open_after.data.into_iter().map(|order| order.id).collect();
            let divergences = finalize_cancel_confirmations(CancelFinalizeContext {
                publisher,
                store,
                execution_reports_stream: &config.execution_reports_stream,
                order_tracker,
                command_id: &command_id,
                source: "cancel_bot_open",
                canceled_order_ids: &response.canceled,
                user_ws_statuses: &user_ws_statuses,
                open_after_ids: &open_after_ids,
            })
            .await?;
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_bot_open_result",
                    "command_id": command_id,
                    "command_type": "cancel_bot_open",
                    "status": cancel_result_status(&divergences, &response.not_canceled),
                    "attempted": order_ids,
                    "canceled": &response.canceled,
                    "not_canceled": &response.not_canceled,
                    "divergences": divergences,
                    "timestamp_ms": now_ms()
                }),
                store,
            )
            .await?;
        }
    }
    Ok(())
}

fn cancel_result_status(
    divergences: &[String],
    not_canceled: &HashMap<String, String>,
) -> &'static str {
    if !divergences.is_empty() {
        "DIVERGED"
    } else if !not_canceled.is_empty() {
        "FAILED"
    } else {
        "CONFIRMED"
    }
}

async fn wait_for_cancel_confirmations(
    store: &StateStore,
    command_id: &str,
    order_ids: &[String],
    timeout_ms: u64,
) -> Result<HashMap<String, String>> {
    let deadline = now_ms().saturating_add(timeout_ms);
    loop {
        let statuses = store.cancel_request_statuses(command_id, order_ids).await?;
        let all_terminal = order_ids.iter().all(|order_id| {
            matches!(
                statuses.get(order_id).map(String::as_str),
                Some("CONFIRMED" | "DIVERGED" | "FAILED")
            )
        });
        if all_terminal || now_ms() >= deadline {
            return Ok(statuses);
        }
        sleep(Duration::from_millis(250)).await;
    }
}

struct CancelFinalizeContext<'a> {
    publisher: &'a mut StreamProducer,
    store: &'a StateStore,
    execution_reports_stream: &'a str,
    order_tracker: &'a OrderTracker,
    command_id: &'a str,
    source: &'a str,
    canceled_order_ids: &'a [String],
    user_ws_statuses: &'a HashMap<String, String>,
    open_after_ids: &'a HashSet<String>,
}

async fn finalize_cancel_confirmations(ctx: CancelFinalizeContext<'_>) -> Result<Vec<String>> {
    let mut divergences = Vec::new();
    for order_id in ctx.canceled_order_ids {
        if matches!(
            ctx.user_ws_statuses.get(order_id).map(String::as_str),
            Some("CONFIRMED")
        ) {
            info!(
                command_id = %ctx.command_id,
                order_id = %order_id,
                "Cancel confirmed by user WebSocket"
            );
            continue;
        }
        if ctx.open_after_ids.contains(order_id) {
            warn!(
                command_id = %ctx.command_id,
                order_id = %order_id,
                "Canceled order still appears in CLOB open orders after confirmation timeout"
            );
            divergences.push(order_id.clone());
            ctx.store
                .record_cancel_request(
                    ctx.command_id,
                    order_id,
                    "DIVERGED",
                    &json!({"source": ctx.source, "reason": "order still open after cancel confirmation timeout"}),
                )
                .await?;
            continue;
        }
        if let Some(order) = resolve_bot_order(ctx.store, ctx.order_tracker, order_id).await? {
            ctx.order_tracker.remove_order(order_id).await;
            publish_cancelled_report(
                ctx.publisher,
                ctx.store,
                ctx.execution_reports_stream,
                order.signal_id,
                order.order_id,
            )
            .await?;
            ctx.store
                .record_cancel_request(
                    ctx.command_id,
                    order_id,
                    "CONFIRMED",
                    &json!({"source": ctx.source, "confirmed_by": "clob_open_orders_fallback"}),
                )
                .await?;
        } else {
            warn!(
                command_id = %ctx.command_id,
                order_id = %order_id,
                "Canceled order could not be reconciled to a bot order"
            );
            divergences.push(order_id.clone());
            ctx.store
                .record_cancel_request(
                    ctx.command_id,
                    order_id,
                    "DIVERGED",
                    &json!({"source": ctx.source, "reason": "unresolvable bot order"}),
                )
                .await?;
        }
    }
    Ok(divergences)
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
        cumulative_filled_size: None,
        remaining_size: Some(0.0),
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
    store: &StateStore,
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
    store.record_control_result(result).await?;
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

    #[test]
    fn cancel_result_status_prioritizes_divergence_then_failure() {
        let mut failed = std::collections::HashMap::new();
        failed.insert("order-1".to_string(), "not found".to_string());

        assert_eq!(
            cancel_result_status(&[], &std::collections::HashMap::new()),
            "CONFIRMED"
        );
        assert_eq!(cancel_result_status(&[], &failed), "FAILED");
        assert_eq!(
            cancel_result_status(&["order-2".to_string()], &failed),
            "DIVERGED"
        );
    }
}
