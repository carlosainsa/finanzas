use anyhow::Result;
use redis::{
    aio::ConnectionManager,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, Client,
};

#[derive(Clone)]
pub struct KeyValueStore {
    conn: ConnectionManager,
}

impl KeyValueStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self { conn })
    }

    pub async fn get_bool(&mut self, key: &str) -> Result<bool> {
        let value: Option<String> = self.conn.get(key).await?;
        Ok(matches!(
            value.as_deref().map(str::to_ascii_lowercase).as_deref(),
            Some("1" | "true" | "yes" | "on")
        ))
    }
}

#[derive(Clone)]
pub struct StreamProducer {
    conn: ConnectionManager,
}

impl StreamProducer {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self { conn })
    }

    pub async fn add_json(&mut self, stream: &str, payload: &str) -> Result<String> {
        let id = self.conn.xadd(stream, "*", &[("payload", payload)]).await?;
        Ok(id)
    }
}

pub struct StreamMessage {
    pub id: String,
    pub payload: String,
}

pub struct StreamConsumer {
    conn: ConnectionManager,
    stream: String,
    group: String,
    consumer: String,
}

impl StreamConsumer {
    pub async fn new(url: &str, stream: &str, group: &str, consumer: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let mut conn = ConnectionManager::new(client).await?;
        ensure_group(&mut conn, stream, group).await?;
        Ok(Self {
            conn,
            stream: stream.to_owned(),
            group: group.to_owned(),
            consumer: consumer.to_owned(),
        })
    }

    pub async fn next_message(&mut self) -> Result<StreamMessage> {
        loop {
            if let Some(message) = self.read_message("0", false).await? {
                return Ok(message);
            }
            if let Some(message) = self.read_message(">", true).await? {
                return Ok(message);
            }
        }
    }

    pub async fn ack(&mut self, id: &str) -> Result<()> {
        self.conn
            .xack::<_, _, _, ()>(&self.stream, &self.group, &[id])
            .await?;
        Ok(())
    }

    async fn read_message(
        &mut self,
        stream_id: &str,
        block: bool,
    ) -> Result<Option<StreamMessage>> {
        let mut opts = StreamReadOptions::default()
            .group(&self.group, &self.consumer)
            .count(1);
        if block {
            opts = opts.block(1_000);
        }
        let reply: StreamReadReply = match self
            .conn
            .xread_options(&[self.stream.as_str()], &[stream_id], &opts)
            .await
        {
            Ok(reply) => reply,
            Err(err) if is_stream_read_timeout(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let Some(key) = reply.keys.first() else {
            return Ok(None);
        };
        let Some(message) = key.ids.first() else {
            return Ok(None);
        };
        let payload = message
            .get::<String>("payload")
            .ok_or_else(|| anyhow::anyhow!("stream message missing payload field"))?;
        Ok(Some(StreamMessage {
            id: message.id.clone(),
            payload,
        }))
    }
}

fn is_stream_read_timeout(err: &redis::RedisError) -> bool {
    err.to_string().to_ascii_lowercase().contains("timed out")
}

async fn ensure_group(conn: &mut ConnectionManager, stream: &str, group: &str) -> Result<()> {
    let result: redis::RedisResult<()> = conn
        .xgroup_create_mkstream(stream, group, consumer_group_start_id())
        .await;
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn consumer_group_start_id() -> &'static str {
    "0"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_group_starts_from_stream_beginning() {
        assert_eq!(consumer_group_start_id(), "0");
    }
}
