use std::collections::LinkedList;

use crate::{cache::Cache, tfchain::tfchain, twin::Twin};
use anyhow::Result;
use futures::StreamExt;
use log;
use subxt::{OnlineClient, PolkadotConfig};

#[derive(Clone)]
pub struct Listener<C>
where
    C: Cache<Twin>,
{
    cache: C,
    api: OnlineClient<PolkadotConfig>,
    substrate_urls: LinkedList<String>,
}

impl<C> Listener<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(substrate_urls: Vec<String>, cache: C) -> Result<Self> {
        let mut urls = LinkedList::new();
        for url in substrate_urls {
            urls.push_back(url);
        }

        let api = Self::connect(&mut urls).await?;

        cache.flush().await?;
        Ok(Listener {
            api,
            cache,
            substrate_urls: urls,
        })
    }

    async fn connect(urls: &mut LinkedList<String>) -> Result<OnlineClient<PolkadotConfig>> {
        let trials = urls.len() * 2;
        for _ in 0..trials {
            let url = match urls.front() {
                Some(url) => url,
                None => anyhow::bail!("substrate urls list is empty"),
            };

            match OnlineClient::<PolkadotConfig>::from_url(url).await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    log::error!(
                        "failed to create substrate client with url \"{}\": {}",
                        url,
                        err
                    );
                }
            }

            if let Some(front) = urls.pop_front() {
                urls.push_back(front);
            }
        }

        anyhow::bail!("failed to connect to substrate using the provided urls")
    }

    pub async fn listen(&mut self) -> Result<()> {
        loop {
            // always flush in case some blocks were finalized before reconnecting
            self.cache.flush().await?;
            match self.handle_events().await {
                Err(err) => {
                    if let Some(subxt::Error::Rpc(_)) = err.downcast_ref::<subxt::Error>() {
                        self.api = Self::connect(&mut self.substrate_urls).await?;
                    } else {
                        return Err(err);
                    }
                }
                Ok(_) => {
                    // reconnect here too?
                    self.api = Self::connect(&mut self.substrate_urls).await?;
                }
            }
        }
    }

    async fn handle_events(&self) -> Result<()> {
        log::info!("started chain events listener");
        let mut blocks_sub = self.api.blocks().subscribe_finalized().await?;
        while let Some(block) = blocks_sub.next().await {
            let events = block?.events().await?;
            for evt in events.iter() {
                let evt = evt?;
                if let Ok(Some(twin)) = evt.as_event::<tfchain::tfgrid_module::events::TwinStored>()
                {
                    self.cache.set(twin.0.id, twin.0.into()).await?;
                } else if let Ok(Some(twin)) =
                    evt.as_event::<tfchain::tfgrid_module::events::TwinUpdated>()
                {
                    self.cache.set(twin.0.id, twin.0.into()).await?;
                }
            }
        }
        Ok(())
    }
}
