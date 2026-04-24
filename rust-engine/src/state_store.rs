use anyhow::Result;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info};

use crate::executor::{ExecutionReport, TradeSignal};

pub struct StateStore {
    client: Option<Client>,
}

pub struct StoredOrder {
    pub signal_id: String,
    pub market_id: String,
    pub asset_id: String,
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
                "insert into orders (order_id, signal_id, market_id, asset_id, status, payload)
                 values ($1, $2, $3, $4, $5, $6)
                 on conflict (order_id) do update set
                    signal_id = excluded.signal_id,
                    market_id = excluded.market_id,
                    asset_id = excluded.asset_id,
                    status = excluded.status,
                    payload = excluded.payload",
                &[
                    &order_id,
                    &signal.signal_id,
                    &signal.market_id,
                    &signal.asset_id,
                    &"SUBMITTED",
                    &payload,
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
                "select signal_id, market_id, asset_id from orders where order_id = $1",
                &[&order_id],
            )
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(StoredOrder {
            signal_id: row.get("signal_id"),
            market_id: row.get("market_id"),
            asset_id: row.get("asset_id"),
        }))
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

    pub async fn record_trade_lifecycle(
        &self,
        trade_id: &str,
        order_id: &str,
        signal_id: &str,
        status: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .execute(
                "insert into trades (trade_id, order_id, signal_id, status, payload)
                 values ($1, $2, $3, $4, $5)
                 on conflict (trade_id) do update set
                    order_id = excluded.order_id,
                    signal_id = excluded.signal_id,
                    status = excluded.status,
                    payload = excluded.payload,
                    updated_at = now()",
                &[&trade_id, &order_id, &signal_id, &status, &payload],
            )
            .await?;
        Ok(())
    }

    async fn migrate(&self) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        client
            .batch_execute(
                "
                create table if not exists trade_signals (
                    signal_id text primary key,
                    market_id text not null,
                    asset_id text not null,
                    payload jsonb not null,
                    created_at timestamptz not null default now()
                );

                create table if not exists execution_reports (
                    signal_id text not null,
                    order_id text not null,
                    status text not null,
                    payload jsonb not null,
                    created_at timestamptz not null default now(),
                    primary key (signal_id, order_id)
                );

                create table if not exists orders (
                    order_id text primary key,
                    signal_id text not null,
                    market_id text not null,
                    asset_id text not null,
                    status text not null,
                    payload jsonb not null,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                );

                create table if not exists trades (
                    trade_id text primary key,
                    order_id text not null,
                    signal_id text not null,
                    status text not null,
                    payload jsonb not null,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                );

                create table if not exists risk_snapshots (
                    id bigserial primary key,
                    payload jsonb not null,
                    created_at timestamptz not null default now()
                );
                ",
            )
            .await?;
        Ok(())
    }
}
