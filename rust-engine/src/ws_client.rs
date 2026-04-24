use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};

use crate::orderbook::{Level, OrderBook};
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

pub async fn run(mut publisher: StreamProducer, url: String, asset_ids: Vec<String>) -> Result<()> {
    info!("Connecting to Polymarket WebSocket: {}", url);

    let (ws_stream, _) = connect_async(&url).await?;
    let (mut write, mut read) = ws_stream.split();

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
                match normalize_book_message(text) {
                    Ok(Some(orderbook)) => {
                        if orderbook.spread().is_none() {
                            error!(
                                market_id = %orderbook.market_id,
                                asset_id = %orderbook.asset_id,
                                "Skipping orderbook without both bid and ask levels"
                            );
                            continue;
                        }
                        let payload = serde_json::to_string(&orderbook)?;
                        publisher.add_json("orderbook:stream", &payload).await?;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        error!(error = %err, raw = %text, "Invalid Polymarket book message")
                    }
                }
            }
            Ok(_) => {}
            Err(e) => error!("WebSocket error: {}", e),
        }
    }

    Ok(())
}

pub(crate) fn normalize_book_message(raw: &str) -> Result<Option<OrderBook>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    if value.get("event_type").and_then(|event| event.as_str()) != Some("book") {
        return Ok(None);
    }

    let book: PolymarketBook = serde_json::from_value(value)?;
    if book.event_type != "book" {
        return Ok(None);
    }

    let mut bids = parse_levels(book.bids)?;
    let mut asks = parse_levels(book.asks)?;
    bids.sort_by(|a, b| b.price.total_cmp(&a.price));
    asks.sort_by(|a, b| a.price.total_cmp(&b.price));

    Ok(Some(OrderBook {
        market_id: book.market,
        asset_id: book.asset_id,
        bids,
        asks,
        timestamp_ms: book.timestamp.parse()?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let book = normalize_book_message(raw).unwrap().unwrap();

        assert_eq!(book.market_id, "0xabc");
        assert_eq!(book.asset_id, "123");
        assert_eq!(book.bids[0].price, 0.43);
        assert_eq!(book.asks[0].price, 0.48);
        assert_eq!(book.timestamp_ms, 1_760_000_000_000);
    }

    #[test]
    fn ignores_non_book_message() {
        let raw = r#"{"event_type":"price_change"}"#;

        assert!(normalize_book_message(raw).unwrap().is_none());
    }
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
