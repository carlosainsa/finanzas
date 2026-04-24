use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};

use crate::config::Config;
use crate::executor::TradeSignal;

#[derive(Debug, Default)]
pub struct RiskService {
    exposure_by_market: HashMap<String, f64>,
    realized_pnl_today: f64,
}

impl RiskService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate(&self, signal: &TradeSignal, config: &Config) -> Result<()> {
        if config.kill_switch {
            bail!("kill switch is enabled");
        }
        if now_ms().saturating_sub(signal.timestamp_ms) > config.signal_max_age_ms {
            bail!("signal is stale");
        }
        if !(0.0..=1.0).contains(&signal.price) {
            bail!("price must be within [0, 1]");
        }
        if signal.size <= 0.0 {
            bail!("size must be positive");
        }
        if signal.size > config.max_order_size {
            bail!("size exceeds MAX_ORDER_SIZE");
        }
        if !(0.0..=1.0).contains(&signal.confidence) {
            bail!("confidence must be within [0, 1]");
        }
        if signal.confidence < config.min_confidence {
            bail!("confidence is below MIN_CONFIDENCE");
        }
        let projected_exposure = self
            .exposure_by_market
            .get(&signal.market_id)
            .copied()
            .unwrap_or_default()
            + signal.size * signal.price;
        if projected_exposure > config.max_market_exposure {
            bail!("market exposure exceeds MAX_MARKET_EXPOSURE");
        }
        if self.realized_pnl_today < -config.max_daily_loss {
            bail!("daily loss exceeds MAX_DAILY_LOSS");
        }
        Ok(())
    }

    pub fn record_accepted_signal(&mut self, signal: &TradeSignal) {
        let exposure = self
            .exposure_by_market
            .entry(signal.market_id.clone())
            .or_default();
        *exposure += signal.size * signal.price;
    }
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
    use crate::config::ExecutionMode;
    use crate::executor::Side;

    fn config() -> Config {
        Config {
            redis_url: "redis://localhost:6379".into(),
            polymarket_ws_url: "ws://localhost".into(),
            polymarket_user_ws_url: "ws://localhost/user".into(),
            polymarket_api_url: "http://localhost".into(),
            database_url: None,
            market_asset_ids: vec![],
            user_market_ids: vec![],
            private_key: None,
            polymarket_api_key: None,
            polymarket_api_secret: None,
            polymarket_api_passphrase: None,
            execution_mode: ExecutionMode::DryRun,
            max_order_size: 10.0,
            min_confidence: 0.55,
            signal_max_age_ms: 5_000,
            max_market_exposure: 100.0,
            max_daily_loss: 50.0,
            kill_switch: false,
            operator_kill_switch_key: "operator:kill_switch".into(),
            order_reconciliation_timeout_ms: 10_000,
        }
    }

    fn signal() -> TradeSignal {
        TradeSignal {
            signal_id: "signal-1".into(),
            market_id: "market-1".into(),
            asset_id: "123".into(),
            side: Side::Buy,
            price: 0.5,
            size: 1.0,
            confidence: 0.9,
            timestamp_ms: now_ms(),
            strategy: Some("test".into()),
        }
    }

    #[test]
    fn rejects_stale_signals() {
        let risk = RiskService::new();
        let mut cfg = config();
        cfg.signal_max_age_ms = 1;
        let mut signal = signal();
        signal.timestamp_ms = 1;

        assert!(risk.validate(&signal, &cfg).is_err());
    }

    #[test]
    fn rejects_kill_switch() {
        let risk = RiskService::new();
        let mut cfg = config();
        cfg.kill_switch = true;

        assert!(risk.validate(&signal(), &cfg).is_err());
    }
}
