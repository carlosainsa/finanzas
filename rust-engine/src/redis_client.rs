use anyhow::Result;
use futures_util::StreamExt;
use redis::{aio::ConnectionManager, AsyncCommands, Client};

/// Publica mensajes en canales Redis. Clonable y seguro para compartir entre tareas.
#[derive(Clone)]
pub struct Publisher {
    conn: ConnectionManager,
}

impl Publisher {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self { conn })
    }

    pub async fn publish(&mut self, channel: &str, message: &str) -> Result<()> {
        self.conn.publish::<_, _, ()>(channel, message).await?;
        Ok(())
    }
}

/// Suscriptor dedicado. No es clonable: cada tarea que necesite suscribirse crea el suyo.
pub struct Subscriber {
    client: Client,
}

impl Subscriber {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        Ok(Self { client })
    }

    pub async fn subscribe(self, channel: &str) -> Result<ActiveSubscriber> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.subscribe(channel).await?;
        Ok(ActiveSubscriber { pubsub })
    }
}

/// Suscriptor activo con canal ya suscrito, listo para iterar mensajes.
pub struct ActiveSubscriber {
    pubsub: redis::aio::PubSub,
}

impl ActiveSubscriber {
    /// Devuelve el siguiente mensaje como String, bloqueando hasta recibirlo.
    pub async fn next_message(&mut self) -> Result<String> {
        let msg = self
            .pubsub
            .on_message()
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Redis pub/sub stream closed"))?;
        Ok(msg.get_payload()?)
    }
}
