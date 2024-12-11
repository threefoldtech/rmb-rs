use std::{collections::LinkedList, time::Duration};

use crate::{cache::Cache, tfchain::tfchain, twin::Twin};
use anyhow::Result;
use futures::StreamExt;
use log;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::select;

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
        let mut urls = LinkedList::from_iter(substrate_urls);

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

    pub async fn listen(&mut self, got_hit: &mut bool) -> Result<()> {
        loop {
            select! {
                _ = tokio::signal::ctrl_c() => {
                    log::info!("shutting down listener gracefully");
                    if let Err(err) = self.cache.flush().await {
                        log::error!("failed to flush redis cache {}", err);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    break;
                },
                result = self.cache.flush() => {
                    // always flush in case some blocks were finalized before reconnecting
                    if let Err(err) = result {
                        log::error!("failed to flush redis cache {}", err);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    if let Err(err) = self.handle_events().await {
                        log::error!("error listening to events {}", err);
                        if let Some(subxt::Error::Rpc(_)) = err.downcast_ref::<subxt::Error>() {
                            self.api = Self::connect(&mut self.substrate_urls).await?;
                        }
                    } else {
                        *got_hit = true
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_events(&self) -> Result<()> {
        log::info!("started chain events listener");
        let mut blocks_sub = self.api.blocks().subscribe_finalized().await?;
        while let Some(block) = blocks_sub.next().await {
            let events = block?.events().await?;
            for evt in events.iter() {
                let evt = match evt {
                    Err(err) => {
                        log::error!("failed to decode event {}", err);
                        continue;
                    }
                    Ok(e) => e,
                };
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
