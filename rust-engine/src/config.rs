use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    pub polymarket_ws_url: String,
    pub polymarket_api_url: String,
    pub market_asset_ids: Vec<String>,
    pub private_key: Option<String>,
    pub execution_mode: ExecutionMode,
    pub max_order_size: f64,
    pub min_confidence: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    DryRun,
    Live,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            redis_url: std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".into()),
            polymarket_ws_url: std::env::var("POLYMARKET_WS_URL")
                .unwrap_or("wss://ws-subscriptions-clob.polymarket.com/ws/market".into()),
            polymarket_api_url: std::env::var("POLYMARKET_API_URL")
                .unwrap_or("https://clob.polymarket.com".into()),
            market_asset_ids: parse_asset_ids("MARKET_ASSET_IDS"),
            private_key: std::env::var("PRIVATE_KEY")
                .or_else(|_| std::env::var("POLYMARKET_PRIVATE_KEY"))
                .ok(),
            execution_mode: ExecutionMode::from_env(),
            max_order_size: parse_env_f64("MAX_ORDER_SIZE", 10.0)?,
            min_confidence: parse_env_f64("MIN_CONFIDENCE", 0.55)?,
        })
    }
}

impl ExecutionMode {
    fn from_env() -> Self {
        match std::env::var("EXECUTION_MODE")
            .unwrap_or_else(|_| "dry_run".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "live" => Self::Live,
            _ => Self::DryRun,
        }
    }
}

fn parse_asset_ids(name: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_env_f64(name: &str, default: f64) -> Result<f64> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}
