use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Alineado con shared/schemas/orderbook.json
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub market_id: String,
    pub asset_id: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
pub struct PriceChange {
    pub market_id: String,
    pub asset_id: String,
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Default)]
pub struct LocalOrderBooks {
    books: HashMap<String, OrderBook>,
}

impl LocalOrderBooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_snapshot(&mut self, snapshot: BookSnapshot) -> Option<OrderBook> {
        let mut book = OrderBook {
            market_id: snapshot.market_id,
            asset_id: snapshot.asset_id.clone(),
            bids: snapshot.bids,
            asks: snapshot.asks,
            timestamp_ms: snapshot.timestamp_ms,
        };
        normalize_book(&mut book);
        self.books.insert(snapshot.asset_id, book.clone());
        book.is_publishable().then_some(book)
    }

    pub fn apply_price_change(&mut self, change: PriceChange) -> Option<OrderBook> {
        let book = self.books.get_mut(&change.asset_id)?;
        if book.market_id != change.market_id || change.timestamp_ms < book.timestamp_ms {
            return None;
        }

        let levels = match change.side {
            BookSide::Bid => &mut book.bids,
            BookSide::Ask => &mut book.asks,
        };
        upsert_level(levels, change.price, change.size);
        book.timestamp_ms = change.timestamp_ms;
        normalize_book(book);
        book.is_publishable().then_some(book.clone())
    }
}

impl OrderBook {
    pub fn is_crossed(&self) -> bool {
        self.spread().is_some_and(|spread| spread <= 0.0)
    }

    pub fn is_publishable(&self) -> bool {
        self.best_bid().is_some() && self.best_ask().is_some() && !self.is_crossed()
    }
}

fn normalize_book(book: &mut OrderBook) {
    book.bids.retain(|level| level.size > 0.0);
    book.asks.retain(|level| level.size > 0.0);
    book.bids.sort_by(|a, b| b.price.total_cmp(&a.price));
    book.asks.sort_by(|a, b| a.price.total_cmp(&b.price));
}

fn upsert_level(levels: &mut Vec<Level>, price: f64, size: f64) {
    if let Some(index) = levels.iter().position(|level| level.price == price) {
        if size == 0.0 {
            levels.remove(index);
        } else {
            levels[index].size = size;
        }
        return;
    }

    if size > 0.0 {
        levels.push(Level { price, size });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> BookSnapshot {
        BookSnapshot {
            market_id: "0xabc".into(),
            asset_id: "123".into(),
            bids: vec![
                Level {
                    price: 0.41,
                    size: 2.0,
                },
                Level {
                    price: 0.43,
                    size: 1.0,
                },
            ],
            asks: vec![
                Level {
                    price: 0.50,
                    size: 1.0,
                },
                Level {
                    price: 0.48,
                    size: 3.0,
                },
            ],
            timestamp_ms: 1_760_000_000_000,
        }
    }

    #[test]
    fn snapshot_sorts_levels() {
        let mut books = LocalOrderBooks::new();
        let book = books.apply_snapshot(snapshot()).unwrap();

        assert_eq!(book.best_bid().unwrap().price, 0.43);
        assert_eq!(book.best_ask().unwrap().price, 0.48);
    }

    #[test]
    fn price_change_updates_existing_level() {
        let mut books = LocalOrderBooks::new();
        books.apply_snapshot(snapshot()).unwrap();

        let book = books
            .apply_price_change(PriceChange {
                market_id: "0xabc".into(),
                asset_id: "123".into(),
                side: BookSide::Bid,
                price: 0.44,
                size: 5.0,
                timestamp_ms: 1_760_000_000_001,
            })
            .unwrap();

        assert_eq!(book.best_bid().unwrap().price, 0.44);
        assert_eq!(book.best_bid().unwrap().size, 5.0);
    }

    #[test]
    fn price_change_zero_size_removes_level() {
        let mut books = LocalOrderBooks::new();
        books.apply_snapshot(snapshot()).unwrap();

        let book = books
            .apply_price_change(PriceChange {
                market_id: "0xabc".into(),
                asset_id: "123".into(),
                side: BookSide::Ask,
                price: 0.48,
                size: 0.0,
                timestamp_ms: 1_760_000_000_001,
            })
            .unwrap();

        assert_eq!(book.best_ask().unwrap().price, 0.50);
    }

    #[test]
    fn stale_price_change_is_ignored() {
        let mut books = LocalOrderBooks::new();
        books.apply_snapshot(snapshot()).unwrap();

        assert!(books
            .apply_price_change(PriceChange {
                market_id: "0xabc".into(),
                asset_id: "123".into(),
                side: BookSide::Bid,
                price: 0.44,
                size: 5.0,
                timestamp_ms: 1,
            })
            .is_none());
    }

    #[test]
    fn crossed_book_is_not_publishable() {
        let mut books = LocalOrderBooks::new();
        assert!(books
            .apply_snapshot(BookSnapshot {
                bids: vec![Level {
                    price: 0.6,
                    size: 1.0,
                }],
                asks: vec![Level {
                    price: 0.5,
                    size: 1.0,
                }],
                ..snapshot()
            })
            .is_none());
    }
}
