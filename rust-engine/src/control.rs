use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
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

        if command.command_type != "cancel_all" {
            consumer.ack(&message.id).await?;
            continue;
        }

        let command_id = command
            .command_id
            .clone()
            .unwrap_or_else(|| format!("legacy-{}", message.id));
        let result = handle_cancel_all(
            &mut publisher,
            &store,
            &config,
            clob.as_ref(),
            &order_tracker,
            &command,
        )
        .await;
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
                    "type": "cancel_all_result",
                    "command_id": command_id,
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
                } else {
                    warn!(
                        order_id = %order_id,
                        "CLOB cancel_all canceled an order not tracked by this runtime"
                    );
                }
            }
            publish_control_result(
                publisher,
                &config.operator_results_stream,
                command,
                &json!({
                    "type": "cancel_all_result",
                    "command_id": command_id,
                    "status": "SENT",
                    "canceled": response.canceled,
                    "not_canceled": response.not_canceled,
                    "timestamp_ms": now_ms()
                }),
            )
            .await?;
        }
    }
    Ok(())
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
                "timestamp_ms": 123
            }"#,
        )
        .unwrap();

        assert_eq!(command.command_type, "cancel_all");
        assert_eq!(command.command_id.as_deref(), Some("command-1"));
        assert_eq!(command.reason.as_deref(), Some("risk off"));
    }
}
