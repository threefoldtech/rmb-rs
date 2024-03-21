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
}

impl<C> Listener<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(url: &str, cache: C) -> Result<Self> {
        let api = OnlineClient::<PolkadotConfig>::from_url(url).await?;
        Ok(Listener { api, cache })
    }
    pub async fn listen(&self) -> Result<()> {
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
