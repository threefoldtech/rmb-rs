use super::RelayDomains;
use super::Twin;
use super::TwinDB;
use crate::cache::Cache;
use crate::identity::Signer;
use crate::identity::Signers;
use anyhow::Result;
use async_trait::async_trait;
use base64::{encode, decode};
use chrono::Utc;
use reqwest::{Client, Error};
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use sp_core::crypto::AccountId32;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct RegistrarTwinDB<C>
where
    C: Cache<Twin>,
{
    client: Arc<Mutex<ClientWrapper>>,
    cache: C,
}

impl<C> RegistrarTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    pub async fn new(registrar_url: String, cache: C) -> Result<Self> {
        let client_wrapper = ClientWrapper::new(registrar_url).await?;

        Ok(Self {
            client: Arc::new(Mutex::new(client_wrapper)),
            cache,
        })
    }

    pub async fn update_twin(
        &self,
        kp: &Signers,
        relay: RelayDomains,
        pk: Option<&[u8]>,
        twin_id: u32,
    ) -> Result<(), Error> {
        let mut client = self.client.lock().await;
        client
            .update_twin(kp, Some(relay.to_string()), pk, twin_id)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C> TwinDB for RegistrarTwinDB<C>
where
    C: Cache<Twin> + Clone,
{
    async fn get_twin(&self, twin_id: u32) -> Result<Option<Twin>> {
        // we can hit the cache as fast as we can here
        if let Some(twin) = self.cache.get(twin_id).await? {
            return Ok(Some(twin));
        }

        let mut client = self.client.lock().await;

        let twin = client.get_twin_by_id(twin_id).await?;

        // but if we wanna hit the grid we get throttled by the workers pool
        // the pool has a limited size so only X queries can be in flight.

        if let Some(ref twin) = twin {
            self.cache.set(twin.id, twin.clone()).await?;
        }

        Ok(twin)
    }

    async fn get_twin_with_account(&self, account_id: AccountId32) -> Result<Option<u32>> {
        let mut client = self.client.lock().await;

        let id = client.get_twin_id_by_account(account_id).await?;

        Ok(id)
    }

    async fn set_twin(&self, twin: Twin) -> Result<()> {
        self.cache.set(twin.id, twin).await?;
        Ok(())
    }
}

/// ClientWrapper is basically a registrar client.
/// all methods exported by the ClientWrapper has a reconnect functionality internally.
/// so if any network error happened, the ClientWrapper will try to reconnect to registrar using the provided registrar urls.
/// if after a number of trials (currently 2 * the number of urls) a reconnect was not successful, the ClientWrapper returns an error.
struct ClientWrapper {
    registrar_url: String,
    client: Client,
}

impl ClientWrapper {
    pub async fn new(registrar_url: String) -> Result<Self> {
        // Create a reqwest client
        let client = Client::new();

        Ok(Self {
            client,
            registrar_url,
        })
    }

    pub async fn update_twin(
        &mut self,
        signer: &Signers,
        relay: Option<String>,
        pk: Option<&[u8]>,
        twin_id: u32,
    ) -> Result<(), Error> {
        let mut update_data = json!({
            "relays": vec![relay.clone()],
            // "rmb_enc_key": ""
        });

        if pk.is_some() {
            let public_key = encode(pk.unwrap());
            update_data["public_key"] = public_key.into();
        }

        let date = Utc::now().timestamp();
        let challenge = format!("{}:{}", date, twin_id);
        let challenge_bytes = challenge.as_bytes();

        let signature_with_prefix: [u8; 65] = signer.sign(challenge_bytes);
        let signature = &signature_with_prefix[1..];

        // Base64 encode the challenge and signature
        let encoded_challenge = encode(challenge_bytes);
        let encoded_signature = encode(signature);

        let response = self.client
            .patch(format!("{}/v1/accounts/{}", self.registrar_url, twin_id))
            .header(
                "X-Auth",
                format!("{}:{}", encoded_challenge, encoded_signature),
            )
            .json(&update_data)
            .send()
            .await?;

        response.error_for_status()?;
        Ok(())
    }

    pub async fn get_twin_by_id(&mut self, twin_id: u32) -> Result<Option<Twin>> {
        let response = self
            .client
            .get(format!(
                "{}/v1/accounts/?twin_id={}",
                self.registrar_url, twin_id
            ))
            .send()
            .await?;

        let body = response.text().await?;
        let twin: RegistrarTwin = serde_json::from_str(&body).map_err(|e| {
            println!("Failed to deserialize JSON: {}", e);
            e
        })?;

        let ss58_address = decode(twin.public_key.clone())?;
        let sized_ss58_address: [u8; 32] = ss58_address.try_into().expect("Failed to convert Vec<u8> to [u8; 32]");
        let account_id = AccountId32::new(sized_ss58_address);

        Ok(Some(Twin { 
            id: twin.twin_id, 
		    account: account_id,
            relay: twin.relays, 
            pk: Some(decode(twin.public_key)?)
        }))
    }

    pub async fn get_twin_id_by_account(&mut self, account_id: AccountId32) -> Result<Option<u32>> {
        let public_key = encode(account_id);

        let response = self
            .client
            .get(format!(
                "{}/v1/accounts/?public_key={}",
                self.registrar_url, public_key
            ))
            .send()
            .await?;

        let body = response.text().await?;
        let twin: RegistrarTwin = serde_json::from_str(&body).map_err(|e| {
            println!("Failed to deserialize JSON: {}", e);
            e
        })?;

        Ok(Some(twin.twin_id))
    }
}


#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RegistrarTwin {
    pub twin_id: u32,
    pub relays: Option<RelayDomains>,
    pub public_key: String,
}

#[cfg(test)]
mod tests {

    use crate::{cache::{MemCache, NoCache}, identity};

    use super::*;
    use anyhow::Context  ;

    #[tokio::test]
    async fn test_get_twin_with_mem_cache() {
        let mem: MemCache<Twin> = MemCache::default();

        let db = RegistrarTwinDB::new(
            String::from("https://registrar.dev4.grid.tf"),
            Some(mem.clone()),
        )
        .await
        .context("cannot create registrar twin db object")
        .unwrap();

        let twin = db
            .get_twin(1)
            .await
            .context("can't get twin from registrar")
            .unwrap()
            .unwrap();

        // NOTE: this currently checks against devnet registrar
        // as provided by the url https://registrar.dev4.grid.tf.
        // if this environment was reset at some point. those
        // values won't match anymore.
        let mut _relays = std::collections::HashSet::new();
        _relays.insert("relay.example.com");
        assert!(matches!(twin.relay, ref _relays));
        let _pk = encode("g3u8KO6eDklBRInndKpm/FtKzJZU7yng5V30tdW9pU8=");
        assert!(matches!(twin.pk, ref _pk));
        assert_eq!(
            twin.account.to_string(),
            "5FHmsyKSevp7nLsA4CDKYe3JVdBibN4KgHKnfg6Jhd3iizcq"
        );

        let cached_twin = mem
            .get::<u32>(1)
            .await
            .context("cannot get twin from the cache")
            .unwrap();

        assert_eq!(Some(twin), cached_twin);
    }

    #[tokio::test]
    async fn test_get_twin_with_no_cache() {
        let db = RegistrarTwinDB::new(String::from("https://registrar.dev4.grid.tf"), NoCache)
            .await
            .context("cannot create registrar twin db object")
            .unwrap();

        let twin = db
            .get_twin(1)
            .await
            .context("can't get twin from registrar")
            .unwrap()
            .unwrap();

        // NOTE: this currently checks against devnet registrar
        // as provided by the url https://registrar.dev4.grid.tf.
        // if this environment was reset at some point. those
        // values won't match anymore.
        let mut _relays = std::collections::HashSet::new();
        _relays.insert("relay.example.com");
        assert!(matches!(twin.relay, ref _relays));
        let _pk = encode("g3u8KO6eDklBRInndKpm/FtKzJZU7yng5V30tdW9pU8=");
        assert!(matches!(twin.pk, ref _pk));
        assert_eq!(
            twin.account.to_string(),
            "5FHmsyKSevp7nLsA4CDKYe3JVdBibN4KgHKnfg6Jhd3iizcq"
        );
    }

    #[tokio::test]
    async fn test_get_twin_id() {
        let db = RegistrarTwinDB::new(String::from("https://registrar.dev4.grid.tf"), NoCache)
            .await
            .context("cannot create registrar twin db object")
            .unwrap();

        let account_id: AccountId32 = "5FHmsyKSevp7nLsA4CDKYe3JVdBibN4KgHKnfg6Jhd3iizcq"
            .parse()
            .unwrap();

        let twin_id = db
            .get_twin_with_account(account_id)
            .await
            .context("can't get twin from registrar")
            .unwrap();

        assert_eq!(Some(1), twin_id);
    }

    #[tokio::test]
    async fn test_update_twin_id() {
        let db = RegistrarTwinDB::new(String::from("https://registrar.dev4.grid.tf"), NoCache)
            .await
            .context("cannot create registrar twin db object")
            .unwrap();

        let secret = "cb0e16d40820466027bf07b6b9d293820b8b1ada687ef961799be2c7ddd483d6837bbc28ee9e0e49414489e774aa66fc5b4acc9654ef29e0e55df4b5d5bda54f";
        let sk = identity::Ed25519Signer::try_from(secret).unwrap();
        let kp = identity::Signers::Ed25519(sk);
        let pk = decode("g3u8KO6eDklBRInndKpm/FtKzJZU7yng5V30tdW9pU8=").unwrap();

        let mut relays = Vec::new();
        relays.push("relay.gent02.dev.grid.tf".to_string());

        db
            .update_twin(&kp, RelayDomains::new(&relays), Some(&pk), 15)
            .await
            .context("can't update twin from registrar")
            .unwrap();

    }
}
