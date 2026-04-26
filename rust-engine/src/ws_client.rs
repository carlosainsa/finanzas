use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::orderbook::{BookSide, BookSnapshot, Level, LocalOrderBooks, OrderBook, PriceChange};
use crate::redis_client::StreamProducer;

#[derive(Debug, Deserialize)]
struct PolymarketBook {
    event_type: String,
    market: String,
    asset_id: String,
    bids: Vec<PolymarketLevel>,
    asks: Vec<PolymarketLevel>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketPriceChangeMessage {
    event_type: String,
    market: String,
    price_changes: Vec<PolymarketPriceChange>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketPriceChange {
    asset_id: String,
    side: String,
    price: String,
    size: String,
}

pub async fn run(mut publisher: StreamProducer, url: String, asset_ids: Vec<String>) -> Result<()> {
    loop {
        if let Err(err) = run_market_ws_once(&mut publisher, &url, &asset_ids).await {
            error!(error = %err, "Polymarket market WebSocket connection failed");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn run_market_ws_once(
    publisher: &mut StreamProducer,
    url: &str,
    asset_ids: &[String],
) -> Result<()> {
    info!("Connecting to Polymarket WebSocket: {}", url);

    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();
    let mut orderbooks = LocalOrderBooks::new();

    if asset_ids.is_empty() {
        error!("MARKET_ASSET_IDS is empty; no Polymarket market data subscription was sent");
    } else {
        let subscription = json!({
            "assets_ids": asset_ids,
            "type": "market",
            "custom_feature_enabled": true
        });
        write.send(Message::Text(subscription.to_string())).await?;
        info!("Subscribed to Polymarket market channel");
    }

    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) if msg.is_text() => {
                let text = msg.to_text()?;
                match process_polymarket_message(text, &mut orderbooks) {
                    Ok(books) => {
                        for book in books {
                            let payload = serde_json::to_string(&book)?;
                            publisher.add_json("orderbook:stream", &payload).await?;
                        }
                    }
                    Err(err) => {
                        error!(error = %err, raw = %text, "Invalid Polymarket market message")
                    }
                }
            }
            Ok(_) => {}
            Err(e) => error!("WebSocket error: {}", e),
        }
    }

    warn!("Polymarket market WebSocket stream ended; reconnecting");
    Ok(())
}

pub(crate) fn process_polymarket_message(
    raw: &str,
    orderbooks: &mut LocalOrderBooks,
) -> Result<Vec<OrderBook>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;

    match value {
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| process_polymarket_value(item, orderbooks))
            .collect::<Result<Vec<_>>>()
            .map(|books| books.into_iter().flatten().collect()),
        value => process_polymarket_value(value, orderbooks).map(|book| book.into_iter().collect()),
    }
}

fn process_polymarket_value(
    value: serde_json::Value,
    orderbooks: &mut LocalOrderBooks,
) -> Result<Option<OrderBook>> {
    match value.get("event_type").and_then(|event| event.as_str()) {
        Some("book") => apply_book_snapshot(value, orderbooks),
        Some("price_change") => apply_price_changes(value, orderbooks),
        _ => Ok(None),
    }
}

fn apply_book_snapshot(
    value: serde_json::Value,
    orderbooks: &mut LocalOrderBooks,
) -> Result<Option<OrderBook>> {
    let book: PolymarketBook = serde_json::from_value(value)?;
    if book.event_type != "book" {
        return Ok(None);
    }

    Ok(orderbooks.apply_snapshot(BookSnapshot {
        market_id: book.market,
        asset_id: book.asset_id,
        bids: parse_levels(book.bids)?,
        asks: parse_levels(book.asks)?,
        timestamp_ms: book.timestamp.parse()?,
    }))
}

fn apply_price_changes(
    value: serde_json::Value,
    orderbooks: &mut LocalOrderBooks,
) -> Result<Option<OrderBook>> {
    let message: PolymarketPriceChangeMessage = serde_json::from_value(value)?;
    if message.event_type != "price_change" {
        return Ok(None);
    }

    let timestamp_ms = message.timestamp.parse()?;
    let mut latest_book = None;
    for change in message.price_changes {
        latest_book = orderbooks.apply_price_change(PriceChange {
            market_id: message.market.clone(),
            asset_id: change.asset_id,
            side: parse_side(&change.side)?,
            price: change.price.parse()?,
            size: change.size.parse()?,
            timestamp_ms,
        });
    }

    Ok(latest_book)
}

fn parse_levels(levels: Vec<PolymarketLevel>) -> Result<Vec<Level>> {
    levels
        .into_iter()
        .map(|level| {
            Ok(Level {
                price: level.price.parse()?,
                size: level.size.parse()?,
            })
        })
        .collect()
}

fn parse_side(side: &str) -> Result<BookSide> {
    match side {
        "BUY" | "buy" => Ok(BookSide::Bid),
        "SELL" | "sell" => Ok(BookSide::Ask),
        _ => anyhow::bail!("invalid price_change side: {side}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_book(orderbooks: &mut LocalOrderBooks) {
        process_polymarket_message(
            r#"{
                "event_type": "book",
                "market": "0xabc",
                "asset_id": "123",
                "bids": [{"price": "0.43", "size": "1"}],
                "asks": [{"price": "0.48", "size": "3"}],
                "timestamp": "1760000000000"
            }"#,
            orderbooks,
        )
        .unwrap();
    }

    #[test]
    fn normalizes_book_message_and_sorts_levels() {
        let raw = r#"{
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "123",
            "bids": [{"price": "0.41", "size": "2"}, {"price": "0.43", "size": "1"}],
            "asks": [{"price": "0.50", "size": "1"}, {"price": "0.48", "size": "3"}],
            "timestamp": "1760000000000"
        }"#;
        let mut orderbooks = LocalOrderBooks::new();

        let book = process_polymarket_message(raw, &mut orderbooks)
            .unwrap()
            .pop()
            .unwrap();

        assert_eq!(book.market_id, "0xabc");
        assert_eq!(book.asset_id, "123");
        assert_eq!(book.bids[0].price, 0.43);
        assert_eq!(book.asks[0].price, 0.48);
        assert_eq!(book.timestamp_ms, 1_760_000_000_000);
    }

    #[test]
    fn ignores_unhandled_message() {
        let raw = r#"{"event_type":"last_trade_price"}"#;
        let mut orderbooks = LocalOrderBooks::new();

        assert!(process_polymarket_message(raw, &mut orderbooks)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn applies_price_change_after_snapshot() {
        let mut orderbooks = LocalOrderBooks::new();
        seed_book(&mut orderbooks);

        let book = process_polymarket_message(
            r#"{
                "event_type": "price_change",
                "market": "0xabc",
                "price_changes": [{
                    "asset_id": "123",
                    "side": "BUY",
                    "price": "0.44",
                    "size": "2"
                }],
                "timestamp": "1760000000001"
            }"#,
            &mut orderbooks,
        )
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, 0.44);
        assert_eq!(book.best_bid().unwrap().size, 2.0);
    }

    #[test]
    fn deletes_zero_sized_price_level() {
        let mut orderbooks = LocalOrderBooks::new();
        seed_book(&mut orderbooks);

        let book = process_polymarket_message(
            r#"{
                "event_type": "price_change",
                "market": "0xabc",
                "price_changes": [{
                    "asset_id": "123",
                    "side": "SELL",
                    "price": "0.48",
                    "size": "0"
                }],
                "timestamp": "1760000000001"
            }"#,
            &mut orderbooks,
        )
        .unwrap();

        assert!(book.is_empty());
    }

    #[test]
    fn ignores_delta_without_snapshot() {
        let mut orderbooks = LocalOrderBooks::new();
        let books = process_polymarket_message(
            r#"{
                "event_type": "price_change",
                "market": "0xabc",
                "price_changes": [{
                    "asset_id": "123",
                    "side": "BUY",
                    "price": "0.44",
                    "size": "2"
                }],
                "timestamp": "1760000000001"
            }"#,
            &mut orderbooks,
        )
        .unwrap();

        assert!(books.is_empty());
    }

    #[test]
    fn applies_array_messages() {
        let raw = r#"[{
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "123",
            "bids": [{"price": "0.43", "size": "1"}],
            "asks": [{"price": "0.48", "size": "3"}],
            "timestamp": "1760000000000"
        }]"#;
        let mut orderbooks = LocalOrderBooks::new();

        assert_eq!(
            process_polymarket_message(raw, &mut orderbooks)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn rejects_invalid_price_change_side() {
        let mut orderbooks = LocalOrderBooks::new();
        seed_book(&mut orderbooks);

        assert!(process_polymarket_message(
            r#"{
                "event_type": "price_change",
                "market": "0xabc",
                "price_changes": [{
                    "asset_id": "123",
                    "side": "BAD",
                    "price": "0.44",
                    "size": "2"
                }],
                "timestamp": "1760000000001"
            }"#,
            &mut orderbooks,
        )
        .is_err());
    }
}
