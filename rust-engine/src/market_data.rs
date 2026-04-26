use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::executor::Side;
use crate::orderbook::OrderBook;

#[derive(Clone, Default)]
pub struct MarketDataCache {
    books: std::sync::Arc<RwLock<HashMap<String, OrderBook>>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DryRunFill {
    pub price: f64,
    pub size: f64,
    pub timestamp_ms: u64,
}

impl MarketDataCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn update(&self, book: OrderBook) {
        self.books.write().await.insert(book.asset_id.clone(), book);
    }

    pub async fn fill_for(
        &self,
        asset_id: &str,
        side: Side,
        limit_price: f64,
        remaining_size: f64,
    ) -> Option<DryRunFill> {
        let books = self.books.read().await;
        let book = books.get(asset_id)?;
        fill_from_book(book, side, limit_price, remaining_size)
    }
}

fn fill_from_book(
    book: &OrderBook,
    side: Side,
    limit_price: f64,
    remaining_size: f64,
) -> Option<DryRunFill> {
    if remaining_size <= 0.0 {
        return None;
    }
    let level = match side {
        Side::Buy => book.best_ask().filter(|level| level.price <= limit_price)?,
        Side::Sell => book.best_bid().filter(|level| level.price >= limit_price)?,
    };
    let size = remaining_size.min(level.size);
    (size > 0.0).then_some(DryRunFill {
        price: level.price,
        size,
        timestamp_ms: book.timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::Level;

    fn book() -> OrderBook {
        OrderBook {
            market_id: "market-1".into(),
            asset_id: "asset-1".into(),
            bids: vec![Level {
                price: 0.44,
                size: 3.0,
            }],
            asks: vec![Level {
                price: 0.46,
                size: 2.0,
            }],
            timestamp_ms: 1_000,
        }
    }

    #[test]
    fn buy_fills_when_best_ask_touches_limit() {
        let fill = fill_from_book(&book(), Side::Buy, 0.46, 5.0).unwrap();

        assert_eq!(fill.price, 0.46);
        assert_eq!(fill.size, 2.0);
    }

    #[test]
    fn sell_fills_when_best_bid_touches_limit() {
        let fill = fill_from_book(&book(), Side::Sell, 0.44, 1.5).unwrap();

        assert_eq!(fill.price, 0.44);
        assert_eq!(fill.size, 1.5);
    }

    #[test]
    fn buy_does_not_fill_above_limit() {
        assert!(fill_from_book(&book(), Side::Buy, 0.45, 5.0).is_none());
    }
}
