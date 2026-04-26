use anyhow::Result;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info};

use crate::executor::{ExecutionReport, TradeSignal};

pub struct StateStore {
    client: Option<Client>,
}

pub struct StoredOrder {
    pub order_id: String,
    pub signal_id: String,
    pub market_id: String,
    pub asset_id: String,
    pub side: Option<String>,
    pub limit_price: Option<f64>,
    pub requested_size: Option<f64>,
    pub filled_size: f64,
    pub remaining_size: Option<f64>,
}

pub struct TradeLifecycleUpdate<'a> {
    pub trade_id: &'a str,
    pub order_id: &'a str,
    pub signal_id: &'a str,
    pub status: &'a str,
    pub payload: &'a serde_json::Value,
    pub fill_price: Option<f64>,
    pub fill_size: Option<f64>,
}

impl StateStore {
    pub fn disabled() -> Self {
        Self { client: None }
    }

    pub async fn connect(database_url: Option<&str>) -> Result<Self> {
        let Some(database_url) = database_url else {
            info!("DATABASE_URL not set; Postgres state store disabled");
            return Ok(Self::disabled());
        };

        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                error!(error = %err, "Postgres connection error");
            }
        });

        let store = Self {
            client: Some(client),
        };
        store.migrate().await?;
        Ok(store)
    }

    pub async fn record_signal(&self, signal: &TradeSignal) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let payload = serde_json::to_value(signal)?;
        client
            .execute(
                "insert into trade_signals (signal_id, market_id, asset_id, payload)
                 values ($1, $2, $3, $4)
                 on conflict (signal_id) do update set payload = excluded.payload",
                &[
                    &signal.signal_id,
                    &signal.market_id,
                    &signal.asset_id,
                    &payload,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn record_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let payload = serde_json::to_value(report)?;
        client
            .execute(
                "insert into execution_reports (signal_id, order_id, status, payload)
                 values ($1, $2, $3, $4)
                 on conflict (signal_id, order_id) do update set
                    status = excluded.status,
                    payload = excluded.payload",
                &[
                    &report.signal_id,
                    &report.order_id,
                    &format!("{:?}", report.status),
                    &payload,
                ],
            )
            .await?;
        self.refresh_position_for_report(report).await?;
        Ok(())
    }

    pub async fn record_order_submission(
        &self,
        signal: &TradeSignal,
        order_id: &str,
    ) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let payload = serde_json::to_value(signal)?;
        client
            .execute(
                "insert into orders (
                    order_id, signal_id, market_id, asset_id, status, payload,
                    requested_size, filled_size, remaining_size
                 )
                 values ($1, $2, $3, $4, $5, $6, $7, 0, $7)
                 on conflict (order_id) do update set
                    signal_id = excluded.signal_id,
                    market_id = excluded.market_id,
                    asset_id = excluded.asset_id,
                    status = excluded.status,
                    payload = excluded.payload,
                    requested_size = excluded.requested_size,
                    remaining_size = coalesce(orders.remaining_size, excluded.remaining_size)",
                &[
                    &order_id,
                    &signal.signal_id,
                    &signal.market_id,
                    &signal.asset_id,
                    &"SUBMITTED",
                    &payload,
                    &signal.size,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn find_order(&self, order_id: &str) -> Result<Option<StoredOrder>> {
        let Some(client) = &self.client else {
            return Ok(None);
        };
        let Some(row) = client
            .query_opt(
                "select
                    signal_id,
                    market_id,
                    asset_id,
                    payload->>'side' as side,
                    (payload->>'price')::double precision as limit_price,
                    requested_size,
                    filled_size,
                    remaining_size
                 from orders where order_id = $1",
                &[&order_id],
            )
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(StoredOrder {
            order_id: order_id.to_owned(),
            signal_id: row.get("signal_id"),
            market_id: row.get("market_id"),
            asset_id: row.get("asset_id"),
            side: row.get("side"),
            limit_price: row.get("limit_price"),
            requested_size: row.get("requested_size"),
            filled_size: row.get("filled_size"),
            remaining_size: row.get("remaining_size"),
        }))
    }

    pub async fn open_bot_orders(&self) -> Result<Vec<StoredOrder>> {
        let Some(client) = &self.client else {
            return Ok(Vec::new());
        };
        let rows = client
            .query(
                "select
                    order_id,
                    signal_id,
                    market_id,
                    asset_id,
                    payload->>'side' as side,
                    (payload->>'price')::double precision as limit_price,
                    requested_size,
                    filled_size,
                    remaining_size
                 from orders
                 where status in ('SUBMITTED', 'DELAYED', 'UNMATCHED', 'PARTIAL', 'Delayed', 'Unmatched', 'Partial')
                    or coalesce(remaining_size, 0) > 0
                 order by updated_at asc",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| StoredOrder {
                order_id: row.get("order_id"),
                signal_id: row.get("signal_id"),
                market_id: row.get("market_id"),
                asset_id: row.get("asset_id"),
                side: row.get("side"),
                limit_price: row.get("limit_price"),
                requested_size: row.get("requested_size"),
                filled_size: row.get("filled_size"),
                remaining_size: row.get("remaining_size"),
            })
            .collect())
    }

    pub async fn record_order_lifecycle(
        &self,
        order_id: &str,
        status: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .execute(
                "update orders
                 set status = $2, payload = $3, updated_at = now()
                 where order_id = $1",
                &[&order_id, &status, &payload],
            )
            .await?;
        Ok(())
    }

    pub async fn record_cancel_request(
        &self,
        command_id: &str,
        order_id: &str,
        status: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        if !matches!(status, "SENT" | "CONFIRMED" | "DIVERGED" | "FAILED") {
            anyhow::bail!("invalid cancel request status: {status}");
        }
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .execute(
                "insert into cancel_requests (command_id, order_id, status, payload)
                 values ($1, $2, $3, $4)
                 on conflict (command_id, order_id) do update set
                    status = excluded.status,
                    payload = excluded.payload,
                    updated_at = now()",
                &[&command_id, &order_id, &status, &payload],
            )
            .await?;
        Ok(())
    }

    pub async fn record_control_result(&self, result: &serde_json::Value) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let command_id = result
            .get("command_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let command_type = result
            .get("command_type")
            .or_else(|| result.get("type"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let status = result
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("UNKNOWN");
        let operator = result.get("operator").and_then(serde_json::Value::as_str);
        let reason = result.get("reason").and_then(serde_json::Value::as_str);
        let error = result.get("error").and_then(serde_json::Value::as_str);
        let command_created_at_ms = result
            .get("command_created_at_ms")
            .and_then(serde_json::Value::as_i64);
        let completed_at_ms = result
            .get("completed_at_ms")
            .and_then(serde_json::Value::as_i64);
        client
            .execute(
                "insert into control_results (
                    command_id, command_type, status, payload, operator, reason,
                    error, command_created_at_ms, completed_at_ms
                 )
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 on conflict (command_id) do update set
                    command_type = excluded.command_type,
                    status = excluded.status,
                    payload = excluded.payload,
                    operator = excluded.operator,
                    reason = excluded.reason,
                    error = excluded.error,
                    command_created_at_ms = excluded.command_created_at_ms,
                    completed_at_ms = excluded.completed_at_ms,
                    updated_at = now()",
                &[
                    &command_id,
                    &command_type,
                    &status,
                    &result,
                    &operator,
                    &reason,
                    &error,
                    &command_created_at_ms,
                    &completed_at_ms,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn confirm_cancel_requests_for_order(
        &self,
        order_id: &str,
        payload: &serde_json::Value,
    ) -> Result<u64> {
        let Some(client) = &self.client else {
            return Ok(0);
        };
        let updated = client
            .execute(
                "update cancel_requests
                 set status = 'CONFIRMED', payload = $2, updated_at = now()
                 where order_id = $1 and status = 'SENT'",
                &[&order_id, &payload],
            )
            .await?;
        Ok(updated)
    }

    pub async fn cancel_request_statuses(
        &self,
        command_id: &str,
        order_ids: &[String],
    ) -> Result<std::collections::HashMap<String, String>> {
        let Some(client) = &self.client else {
            return Ok(std::collections::HashMap::new());
        };
        if order_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let rows = client
            .query(
                "select order_id, status from cancel_requests
                 where command_id = $1 and order_id = any($2)",
                &[&command_id, &order_ids],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("order_id"), row.get("status")))
            .collect())
    }

    pub async fn record_trade_lifecycle(&self, update: TradeLifecycleUpdate<'_>) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .execute(
                "insert into trades (trade_id, order_id, signal_id, status, payload, fill_price, fill_size)
                 values ($1, $2, $3, $4, $5, $6, $7)
                 on conflict (trade_id) do update set
                    order_id = excluded.order_id,
                    signal_id = excluded.signal_id,
                    status = excluded.status,
                    payload = excluded.payload,
                    fill_price = excluded.fill_price,
                    fill_size = excluded.fill_size,
                    updated_at = now()",
                &[
                    &update.trade_id,
                    &update.order_id,
                    &update.signal_id,
                    &update.status,
                    &update.payload,
                    &update.fill_price,
                    &update.fill_size,
                ],
            )
            .await?;
        client
            .execute(
                "update orders
                 set
                    filled_size = coalesce((
                        select sum(fill_size)
                        from trades
                        where trades.order_id = orders.order_id
                          and trades.status in ('MATCHED', 'MINED', 'CONFIRMED')
                    ), 0),
                    remaining_size = greatest(
                        coalesce(requested_size, (payload->>'size')::double precision, 0)
                        - coalesce((
                            select sum(fill_size)
                            from trades
                            where trades.order_id = orders.order_id
                              and trades.status in ('MATCHED', 'MINED', 'CONFIRMED')
                        ), 0),
                        0
                    ),
                    status = case
                        when greatest(
                            coalesce(requested_size, (payload->>'size')::double precision, 0)
                            - coalesce((
                                select sum(fill_size)
                                from trades
                                where trades.order_id = orders.order_id
                                  and trades.status in ('MATCHED', 'MINED', 'CONFIRMED')
                            ), 0),
                            0
                        ) = 0 then 'MATCHED'
                        else 'PARTIAL'
                    end,
                    updated_at = now()
                 where order_id = $1",
                &[&update.order_id],
            )
            .await?;
        Ok(())
    }

    async fn migrate(&self) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let migration_lock_id = 724_834_072_344_i64;
        client
            .execute("select pg_advisory_lock($1)", &[&migration_lock_id])
            .await?;
        client
            .batch_execute(
                "create table if not exists schema_migrations (
                    version text primary key,
                    applied_at timestamptz not null default now()
                );",
            )
            .await?;
        for (version, sql) in migrations() {
            let exists = client
                .query_opt(
                    "select version from schema_migrations where version = $1",
                    &[&version],
                )
                .await?
                .is_some();
            if exists {
                continue;
            }
            client.batch_execute(sql).await?;
            client
                .execute(
                    "insert into schema_migrations (version) values ($1)",
                    &[&version],
                )
                .await?;
        }
        client
            .execute("select pg_advisory_unlock($1)", &[&migration_lock_id])
            .await?;
        Ok(())
    }

    async fn refresh_position_for_report(&self, report: &ExecutionReport) -> Result<()> {
        if !matches!(
            report.status,
            crate::executor::ExecutionStatus::Matched | crate::executor::ExecutionStatus::Partial
        ) {
            return Ok(());
        }
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .execute(
                "insert into positions (market_id, asset_id, position)
                 select
                    ts.market_id,
                    ts.asset_id,
                    sum(
                        case
                            when ts.payload->>'side' = 'BUY' then
                                coalesce(
                                    (er.payload->>'cumulative_filled_size')::double precision,
                                    (er.payload->>'filled_size')::double precision,
                                    0
                                )
                            else
                                -coalesce(
                                    (er.payload->>'cumulative_filled_size')::double precision,
                                    (er.payload->>'filled_size')::double precision,
                                    0
                                )
                        end
                    ) as position
                 from execution_reports er
                 join trade_signals ts on ts.signal_id = er.signal_id
                 where er.status in ('Matched', 'MATCHED', 'Partial', 'PARTIAL')
                   and ts.market_id = (
                       select market_id from trade_signals where signal_id = $1
                   )
                   and ts.asset_id = (
                       select asset_id from trade_signals where signal_id = $1
                   )
                 group by ts.market_id, ts.asset_id
                 on conflict (market_id, asset_id) do update set
                    position = excluded.position,
                    updated_at = now()",
                &[&report.signal_id],
            )
            .await?;
        Ok(())
    }
}

fn migrations() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "0001_initial",
            include_str!("../../shared/migrations/0001_initial.sql"),
        ),
        (
            "0002_cancel_requests_positions",
            include_str!("../../shared/migrations/0002_cancel_requests_positions.sql"),
        ),
        (
            "0003_cancel_request_status_constraint",
            include_str!("../../shared/migrations/0003_cancel_request_status_constraint.sql"),
        ),
        (
            "0004_order_fill_state",
            include_str!("../../shared/migrations/0004_order_fill_state.sql"),
        ),
        (
            "0005_control_results",
            include_str!("../../shared/migrations/0005_control_results.sql"),
        ),
        (
            "0006_control_result_audit_fields",
            include_str!("../../shared/migrations/0006_control_result_audit_fields.sql"),
        ),
    ]
}
