use anyhow::Result;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info};

use crate::executor::{ExecutionReport, TradeSignal};

pub struct StateStore {
    client: Option<Client>,
}

impl StateStore {
    pub async fn connect(database_url: Option<&str>) -> Result<Self> {
        let Some(database_url) = database_url else {
            info!("DATABASE_URL not set; Postgres state store disabled");
            return Ok(Self { client: None });
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
