use std::str::FromStr;

use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use polymarket_client_sdk::auth::Signer as _;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;

use crate::config::Config;

pub type AuthenticatedClient = polymarket_client_sdk::clob::Client<
    polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
>;

pub struct AuthenticatedClob {
    pub signer: PrivateKeySigner,
    pub client: AuthenticatedClient,
}

impl AuthenticatedClob {
    pub async fn from_config(config: &Config) -> Result<Self> {
        let private_key = config
            .private_key
            .as_deref()
            .context("PRIVATE_KEY or POLYMARKET_PRIVATE_KEY is required in live mode")?;
        let signer = PrivateKeySigner::from_str(private_key)?.with_chain_id(Some(POLYGON));
        let client = ClobClient::new(
            &config.polymarket_api_url,
            ClobConfig::builder().use_server_time(true).build(),
        )?
        .authentication_builder(&signer)
        .authenticate()
        .await?;
        Ok(Self { signer, client })
    }
}
