use serde::{Deserialize, Serialize};

/// Alineado con shared/schemas/orderbook.json
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderBook {
    pub market_id: String,
    pub asset_id: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub timestamp_ms: u64,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<&Level> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&Level> {
        self.asks.first()
    }

    pub fn spread(&self) -> Option<f64> {
        Some(self.best_ask()?.price - self.best_bid()?.price)
    }
}
