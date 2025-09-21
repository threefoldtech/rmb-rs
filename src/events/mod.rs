use std::{collections::VecDeque, time::Duration};

use crate::{cache::Cache, tfchain::tfchain, twin::Twin};
use anyhow::Result;
use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use log;
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use subxt::{blocks::Block as SubxtBlock, config::Header, OnlineClient, PolkadotConfig};

lazy_static! {
    static ref EVENTS_RECONNECTING: IntGauge = IntGauge::new(
        "events_listener_reconnecting",
        "1 while reconnecting/backing off; 0 otherwise",
    )
    .unwrap();
    static ref EVENTS_RECONNECT_CYCLES: IntCounter = IntCounter::new(
        "events_listener_reconnect_cycles_total",
        "Successful reconnect cycles",
    )
    .unwrap();
    static ref EVENTS_ERRORS: IntCounterVec = IntCounterVec::new(
        Opts::new("events_listener_errors_total", "Errors by stage"),
        &["stage"],
    )
    .unwrap();
    static ref EVENTS_LAST_BLOCK_NUM: IntGauge = IntGauge::new(
        "events_listener_last_processed_block_number",
        "Last best-head block number processed",
    )
    .unwrap();
    static ref EVENTS_TWIN_STORED_TOTAL: IntCounter = IntCounter::new(
        "events_listener_twin_stored_total",
        "Total number of TwinStored events processed",
    )
    .unwrap();
    static ref EVENTS_TWIN_UPDATED_TOTAL: IntCounter = IntCounter::new(
        "events_listener_twin_updated_total",
        "Total number of TwinUpdated events processed",
    )
    .unwrap();
}

pub struct EventListenerOptions {
    catchup_threshold: u64,
    registry: Registry,
}

impl Default for EventListenerOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl EventListenerOptions {
    pub fn new() -> Self {
        Self {
            catchup_threshold: 600,
            registry: prometheus::default_registry().clone(),
        }
    }

    pub fn with_catchup_threshold(mut self, v: u64) -> Self {
        self.catchup_threshold = v;
        self
    }

    pub fn with_registry(mut self, r: Registry) -> Self {
        self.registry = r;
        self
    }

    pub async fn build<C>(self, substrate_urls: Vec<String>, cache: C) -> Result<Listener<C>>
    where
        C: Cache<Twin> + Clone,
    {
        let mut urls: VecDeque<String> = substrate_urls.into();
        let api = Listener::<C>::connect(&mut urls).await?;

        // flush cache on startup to make sure we start with a clean state
        cache.flush().await?;

        // Register metrics into provided registry (ignore duplicate registration errors)
        let _ = self
            .registry
            .register(Box::new(EVENTS_RECONNECTING.clone()));
        let _ = self
            .registry
            .register(Box::new(EVENTS_RECONNECT_CYCLES.clone()));
        let _ = self.registry.register(Box::new(EVENTS_ERRORS.clone()));
        let _ = self
            .registry
            .register(Box::new(EVENTS_LAST_BLOCK_NUM.clone()));
        let _ = self
            .registry
            .register(Box::new(EVENTS_TWIN_STORED_TOTAL.clone()));
        let _ = self
            .registry
            .register(Box::new(EVENTS_TWIN_UPDATED_TOTAL.clone()));

        Ok(Listener {
            api,
            cache,
            substrate_urls: urls,
            last_processed: None,
            catchup_threshold: self.catchup_threshold,
        })
    }
}

pub struct Listener<C>
where
    C: Cache<Twin>,
{
    cache: C,
    api: OnlineClient<PolkadotConfig>,
    substrate_urls: VecDeque<String>,
    // In-memory catch-up state
    last_processed: Option<u64>,
    catchup_threshold: u64,
}

impl<C> Listener<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(substrate_urls: Vec<String>, cache: C) -> Result<Self> {
        // Delegate to options with defaults (threshold=600, default registry)
        EventListenerOptions::new()
            .build(substrate_urls, cache)
            .await
    }

    async fn connect(urls: &mut VecDeque<String>) -> Result<OnlineClient<PolkadotConfig>> {
        let trials = urls.len() * 2;
        for _ in 0..trials {
            let url = match urls.front() {
                Some(url) => url,
                None => anyhow::bail!("substrate urls list is empty"),
            };

            // Protect against hangs in DNS/TLS/WebSocket setup
            let attempt_timeout = Duration::from_secs(12);
            match tokio::time::timeout(
                attempt_timeout,
                // JSON-RPC requests governed by jsonrpsee’s client request timeout (default 60s unless you configure it).
                // Connection timing is bounded separately by sonrpsee_client_transport::ws::WsTransportClientBuilder::connection_timeout (default 10s unless you configure it).
                OnlineClient::<PolkadotConfig>::from_url(url),
            )
            .await
            {
                Ok(Ok(client)) => return Ok(client),
                Ok(Err(err)) => {
                    log::error!(
                        "failed to create substrate client with url \"{}\": {}",
                        url,
                        err
                    );
                }
                Err(_) => {
                    log::error!(
                        "connect to \"{}\" timed out after {:?}",
                        url,
                        attempt_timeout
                    );
                }
            }

            if let Some(front) = urls.pop_front() {
                urls.push_back(front);
            }
        }

        anyhow::bail!("failed to connect to substrate using the provided urls")
    }

    /// reconnects to the chain and ensures it's fully caught up.
    async fn reconnect(&mut self) -> Result<()> {
        self.api = Self::connect(&mut self.substrate_urls).await?;

        // This loop ensures we catch up to the absolute latest block,
        // even if new blocks are produced during the sync process.
        loop {
            let head_block = self.api.blocks().at_latest().await?;
            let head_num: u64 = head_block.header().number().into();

            let last = match self.last_processed {
                None => {
                    // No prior state: set to head and we are done.
                    self.last_processed = Some(head_num);
                    return Ok(());
                }
                Some(last) => last,
            };

            if head_num <= last {
                // We are fully caught up, exit the loop.
                log::info!("successfully caught up to block {}", last);
                return Ok(());
            }

            // --- CATCH-UP LOGIC ---
            let gap = head_num - last;
            if gap > self.catchup_threshold {
                log::warn!(
                    "gap {} exceeds threshold {}; flushing cache and jumping to head {}",
                    gap,
                    self.catchup_threshold,
                    head_num
                );
                self.cache.flush().await?;
                self.last_processed = Some(head_num);
                // After a flush, we consider ourselves caught up to the new head.
                return Ok(());
            }

            log::info!("catching up {} blocks ({} -> {})", gap, last, head_num);

            // --- BATCH PROCESSING LOGIC ---
            const BATCH_SIZE: usize = 20; // Process 20 blocks in parallel
            let block_numbers_to_process = (last + 1)..=head_num;

            // Clone API to avoid holding &mut self across awaited tasks
            let api = self.api.clone();
            let mut block_stream = stream::iter(block_numbers_to_process)
                .map(move |n| {
                    let api = api.clone();
                    async move {
                        let hash = api.rpc().block_hash(Some(n.into())).await?;
                        let block_opt = match hash {
                            Some(h) => Some(api.blocks().at(h).await?),
                            None => None,
                        };
                        anyhow::Result::<
                            Option<SubxtBlock<PolkadotConfig, OnlineClient<PolkadotConfig>>>,
                        >::Ok(block_opt)
                    }
                })
                .buffered(BATCH_SIZE);

            while let Some(result) = block_stream.next().await {
                match result {
                    Ok(Some(block)) => self.process_block(block).await?,
                    Ok(None) => log::warn!("Could not find block during catch-up"),
                    Err(e) => {
                        log::warn!("catch-up fetch error; re-establishing and retrying: {}", e);
                        // Recursively attempt to reconnect and retry catch-up, avoiding outer backoff.
                        return Err(e);
                    }
                }
            }
            // After processing the batch, the loop will restart to check the head again.
        }
    }

    pub async fn listen(&mut self) -> Result<()> {
        // Exponential backoff with jitter after reconnect failures; reset on successful reconnect.
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(6);
        log::info!("started chain events listener");
        loop {
            if let Err(err) = self.handle_events_inner().await {
                log::warn!("event handler failed: {}", err);
                EVENTS_RECONNECTING.set(1);
                // Keep retrying reconnect until success, with capped exponential backoff
                loop {
                    match self.reconnect().await {
                        Ok(_) => {
                            log::info!("event handler successfully reconnected");
                            backoff = Duration::from_secs(1);
                            EVENTS_RECONNECT_CYCLES.inc();
                            EVENTS_RECONNECTING.set(0);
                            break; // exit reconnect loop and resume handling events
                        }
                        Err(reconnect_err) => {
                            log::error!(
                                "failed to reconnect after event handler failure: {}; retrying in {:?}",
                                reconnect_err,
                                backoff
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                        }
                    }
                }
            }
        }
    }

    async fn handle_events_inner(&mut self) -> Result<()> {
        // Subscribe to best head to minimize latency;
        let mut blocks_sub = self.api.blocks().subscribe_best().await?;
        // Detect stalled streams: if no new block arrives within this window, bail to trigger reconnect.
        let stall_timeout = Duration::from_secs(7);
        while let Some(block) = match tokio::time::timeout(stall_timeout, blocks_sub.next()).await {
            Ok(v) => v,
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "block subscription timed out after {:?}",
                    stall_timeout
                ));
            }
        } {
            let block = block?;
            self.process_block(block).await?;
        }
        Ok(())
    }

    // Unified per-block processing: updates metrics, last_processed, and handles events
    async fn process_block(
        &mut self,
        block: SubxtBlock<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    ) -> Result<()> {
        let header = block.header();
        let num_i64: i64 = header.number.into();
        EVENTS_LAST_BLOCK_NUM.set(num_i64);
        log::trace!("processing block number: {}", num_i64);

        let events = block.events().await?;
        for evt in events.iter() {
            let evt = match evt {
                Err(err) => {
                    log::error!("failed to decode event {}", err);
                    EVENTS_ERRORS.with_label_values(&["decode"]).inc();
                    continue;
                }
                Ok(e) => e,
            };
            if let Ok(Some(twin)) = evt.as_event::<tfchain::tfgrid_module::events::TwinStored>() {
                self.cache
                    .set(twin.0.id, twin.0.into())
                    .await
                    .inspect_err(|_| {
                        EVENTS_ERRORS.with_label_values(&["cache_set"]).inc();
                    })?;
                EVENTS_TWIN_STORED_TOTAL.inc();
            } else if let Ok(Some(twin)) =
                evt.as_event::<tfchain::tfgrid_module::events::TwinUpdated>()
            {
                self.cache
                    .set(twin.0.id, twin.0.into())
                    .await
                    .inspect_err(|_| {
                        EVENTS_ERRORS.with_label_values(&["cache_set"]).inc();
                    })?;
                EVENTS_TWIN_UPDATED_TOTAL.inc();
            }
        }
        self.last_processed = Some(num_i64 as u64);
        Ok(())
    }
}
