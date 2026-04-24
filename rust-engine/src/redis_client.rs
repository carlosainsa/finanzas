use anyhow::Result;
use redis::{
    aio::ConnectionManager,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, Client,
};

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
                .block(5_000);
            let reply: StreamReadReply = self
                .conn
                .xread_options(&[self.stream.as_str()], &[">"], &opts)
                .await?;

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

async fn ensure_group(conn: &mut ConnectionManager, stream: &str, group: &str) -> Result<()> {
    let result: redis::RedisResult<()> = conn.xgroup_create_mkstream(stream, group, "$").await;
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(err.into()),
    }
}
