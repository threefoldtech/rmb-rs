use anyhow::{Context, Result};
/// proxy is a special worker that accept handle messages with command system.proxy.
/// those messages than are specially treated according to specs
use std::{sync::Arc, time::Duration};

use crate::{
    identity::Signer,
    storage::ProxyStorage,
    twin::TwinDB,
    types::{Message, TransitMessage},
};

use workers::{Work, WorkerPool};

struct Worker<S, T, I> {
    id: u32,
    storage: S,
    db: T,
    identity: I,
}

impl<S, T, I> Worker<S, T, I> {
    pub fn new(id: u32, storage: S, db: T, identity: I) -> Self {
        Worker {
            id,
            storage,
            db,
            identity,
        }
    }
}

impl<S, T, I> Worker<S, T, I>
where
    S: ProxyStorage,
    T: TwinDB,
    I: Signer,
{
    async fn request_handler(&self, envelope: &Message) -> Result<()> {
        // if we are here this is a msg with command system.proxy.
        // all envelope validation is complete. But we need now to extract
        // the payload of the message. and then validate this as a separate message
        let payload = base64::decode(&envelope.data).context("failed to decode payload")?;
        let mut payload = Message::from_json(&payload).context("invalid payload message")?;

        if !payload.destination.iter().any(|x| *x == self.id) {
            // this message is not intended to this destination
            // and this is a violation that is not accepted
            bail!("invalid payload message destination");
        }

        payload
            .valid()
            .context("payload message validation failed")?;

        let twin = self
            .db
            .get_twin(payload.source)
            .await
            .context("failed to get twin")?
            .ok_or_else(|| anyhow!("destination twin not found"))?;

        // verify before modifying the message
        payload
            .verify(twin.account)
            .context("payload verification failed")?;

        payload.id = envelope.id.clone();

        // save the envelope separately
        self.storage.set_envelope(envelope).await?;
        // reply queue from storage
        self.storage.run_proxied(payload).await
    }

    async fn handle_err(&self, mut msg: Message, err: anyhow::Error) {
        msg.error = Some(format!("{:#}", err));
        msg.data = String::default();

        if let Err(err) = self.storage.response(&msg).await {
            log::error!("failed to push proxy response: {}", err);
        }
    }

    async fn request(&self, msg: Message) {
        if let Err(err) = self.request_handler(&msg).await {
            self.handle_err(msg, err).await;
        }
    }

    async fn reply_handler(&self, mut msg: Message) -> Result<()> {
        let mut envelope = match self.storage.get_envelope(&msg.id).await? {
            Some(envelope) => envelope,
            None => {
                // timed-out .. nothing to do.
                log::debug!("envelope of {} is now expired", &msg.id);
                return Ok(());
            }
        };

        // sign the message
        msg.sign(&self.identity);

        envelope.data = base64::encode(msg.to_json().context("failed to encode message")?);
        // swap the source and destination
        let source = envelope.source;
        envelope.source = envelope.destination[0];
        envelope.destination = vec![source];
        // send for normal reply
        self.storage.response(&envelope).await
    }

    async fn reply(&self, msg: Message) {
        if let Err(err) = self.reply_handler(msg).await {
            log::error!("failed to handle proxy reply: {}", err)
        }
    }
}

#[async_trait::async_trait]
impl<S, T, I> Work for Worker<S, T, I>
where
    S: ProxyStorage,
    T: TwinDB,
    I: Signer,
{
    type Input = TransitMessage;
    type Output = ();

    async fn run(&self, job: Self::Input) {
        match job {
            TransitMessage::Request(msg) => self.request(msg).await,
            TransitMessage::Reply(msg) => self.reply(msg).await,
            // cannot proxy uploads
            TransitMessage::Upload(_) => (),
        }
    }
}

pub struct ProxyWorker<S, T, I>
where
    S: ProxyStorage,
    T: TwinDB,
    I: Signer,
{
    storage: S,
    pool: WorkerPool<Arc<Worker<S, T, I>>>,
}

impl<S, T, I> ProxyWorker<S, T, I>
where
    S: ProxyStorage,
    T: TwinDB,
    I: Signer + 'static,
{
    pub fn new(id: u32, size: usize, storage: S, twin_db: T, identity: I) -> Self {
        // it's cheaper to create one http client and then clone it to the workers
        // according to docs this will make it share the same connection pool.
        let worker = Worker::new(id, storage.clone(), twin_db, identity);
        let pool = WorkerPool::new(Arc::new(worker), size);
        Self { storage, pool }
    }

    pub async fn run(mut self) {
        loop {
            let handler = self.pool.get().await;

            let job = match self.storage.proxied().await {
                Ok(job) => job,
                Err(err) => {
                    log::debug!("error while process the storage because of '{}'", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            if let Err(err) = handler.send(job) {
                log::error!("failed to send job to worker: {}", err);
            }
        }
    }
}
