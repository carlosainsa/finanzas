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
            let opts = StreamReadOptions::default()
                .group(&self.group, &self.consumer)
                .count(1)
                .block(1_000);
            let reply: StreamReadReply = match self
                .conn
                .xread_options(&[self.stream.as_str()], &[">"], &opts)
                .await
            {
                Ok(reply) => reply,
                Err(err) if is_stream_read_timeout(&err) => continue,
                Err(err) => return Err(err.into()),
            };

            let Some(key) = reply.keys.first() else {
                continue;
            };
            let Some(message) = key.ids.first() else {
                continue;
            };
            let payload = message
                .get::<String>("payload")
                .ok_or_else(|| anyhow::anyhow!("stream message missing payload field"))?;
            return Ok(StreamMessage {
                id: message.id.clone(),
                payload,
            });
        }
    }

    pub async fn ack(&mut self, id: &str) -> Result<()> {
        self.conn
            .xack::<_, _, _, ()>(&self.stream, &self.group, &[id])
            .await?;
        Ok(())
    }
}

fn is_stream_read_timeout(err: &redis::RedisError) -> bool {
    err.to_string().to_ascii_lowercase().contains("timed out")
}

async fn ensure_group(conn: &mut ConnectionManager, stream: &str, group: &str) -> Result<()> {
    let result: redis::RedisResult<()> = conn.xgroup_create_mkstream(stream, group, "$").await;
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err.into()),
    }
}
