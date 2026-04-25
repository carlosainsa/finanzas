use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    pub app_env: String,
    pub polymarket_ws_url: String,
    pub polymarket_user_ws_url: String,
    pub polymarket_api_url: String,
    pub database_url: Option<String>,
    pub market_asset_ids: Vec<String>,
    pub user_market_ids: Vec<String>,
    pub private_key: Option<String>,
    pub polymarket_api_key: Option<String>,
    pub polymarket_api_secret: Option<String>,
    pub polymarket_api_passphrase: Option<String>,
    pub execution_mode: ExecutionMode,
    pub max_order_size: f64,
    pub min_confidence: f64,
    pub signal_max_age_ms: u64,
    pub max_market_exposure: f64,
    pub max_daily_loss: f64,
    pub kill_switch: bool,
    pub operator_kill_switch_key: String,
    pub order_reconciliation_timeout_ms: u64,
    pub signals_stream: String,
    pub execution_reports_stream: String,
    pub operator_commands_stream: String,
    pub operator_results_stream: String,
    pub executor_consumer_group: String,
    pub executor_consumer_name: String,
    pub operator_consumer_group: String,
    pub operator_consumer_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    DryRun,
    Live,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self {
            redis_url: std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".into()),
            app_env: std::env::var("APP_ENV").unwrap_or("development".into()),
            polymarket_ws_url: std::env::var("POLYMARKET_WS_URL")
                .unwrap_or("wss://ws-subscriptions-clob.polymarket.com/ws/market".into()),
            polymarket_user_ws_url: std::env::var("POLYMARKET_USER_WS_URL")
                .unwrap_or("wss://ws-subscriptions-clob.polymarket.com/ws/user".into()),
            polymarket_api_url: std::env::var("POLYMARKET_API_URL")
                .unwrap_or("https://clob.polymarket.com".into()),
            database_url: std::env::var("DATABASE_URL").ok(),
            market_asset_ids: parse_asset_ids("MARKET_ASSET_IDS"),
            user_market_ids: parse_asset_ids("USER_MARKET_IDS"),
            private_key: std::env::var("PRIVATE_KEY")
                .or_else(|_| std::env::var("POLYMARKET_PRIVATE_KEY"))
                .ok(),
            polymarket_api_key: std::env::var("POLYMARKET_API_KEY").ok(),
            polymarket_api_secret: std::env::var("POLYMARKET_API_SECRET").ok(),
            polymarket_api_passphrase: std::env::var("POLYMARKET_API_PASSPHRASE").ok(),
            execution_mode: ExecutionMode::from_env(),
            max_order_size: parse_env_f64("MAX_ORDER_SIZE", 10.0)?,
            min_confidence: parse_env_f64("MIN_CONFIDENCE", 0.55)?,
            signal_max_age_ms: parse_env_u64("SIGNAL_MAX_AGE_MS", 5_000)?,
            max_market_exposure: parse_env_f64("MAX_MARKET_EXPOSURE", 100.0)?,
            max_daily_loss: parse_env_f64("MAX_DAILY_LOSS", 50.0)?,
            kill_switch: parse_env_bool("KILL_SWITCH", false),
            operator_kill_switch_key: std::env::var("OPERATOR_KILL_SWITCH_KEY")
                .unwrap_or("operator:kill_switch".into()),
            order_reconciliation_timeout_ms: parse_env_u64(
                "ORDER_RECONCILIATION_TIMEOUT_MS",
                10_000,
            )?,
            signals_stream: std::env::var("SIGNALS_STREAM").unwrap_or("signals:stream".into()),
            execution_reports_stream: std::env::var("EXECUTION_REPORTS_STREAM")
                .unwrap_or("execution:reports:stream".into()),
            operator_commands_stream: std::env::var("OPERATOR_COMMANDS_STREAM")
                .unwrap_or("operator:commands:stream".into()),
            operator_results_stream: std::env::var("OPERATOR_RESULTS_STREAM")
                .unwrap_or("operator:results:stream".into()),
            executor_consumer_group: std::env::var("EXECUTOR_CONSUMER_GROUP")
                .unwrap_or("rust-executor".into()),
            executor_consumer_name: std::env::var("EXECUTOR_CONSUMER_NAME")
                .unwrap_or("executor-1".into()),
            operator_consumer_group: std::env::var("OPERATOR_CONSUMER_GROUP")
                .unwrap_or("rust-control".into()),
            operator_consumer_name: std::env::var("OPERATOR_CONSUMER_NAME")
                .unwrap_or("control-1".into()),
        }
        .validate()
    }

    fn validate(self) -> Result<Self> {
        if self.app_env.eq_ignore_ascii_case("production") {
            let mut missing = Vec::new();
            if self.database_url.is_none() {
                missing.push("DATABASE_URL");
            }
            if std::env::var("OPERATOR_READ_TOKEN").is_err() {
                missing.push("OPERATOR_READ_TOKEN");
            }
            if std::env::var("OPERATOR_CONTROL_TOKEN").is_err() {
                missing.push("OPERATOR_CONTROL_TOKEN");
            }
            if std::env::var("EXECUTION_MODE").is_err() {
                missing.push("EXECUTION_MODE");
            }
            if !missing.is_empty() {
                anyhow::bail!(
                    "production configuration missing required settings: {}",
                    missing.join(", ")
                );
            }
        }
        Ok(self)
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

fn parse_env_u64(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn parse_env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}
