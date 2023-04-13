mod ed25519;
mod sr25519;

use std::{fmt::Display, str::FromStr};

pub use ed25519::{Ed25519Identity, Ed25519Signer, PREFIX as ED_PREFIX};
pub use sr25519::{Sr25519Identity, Sr25519Signer, PREFIX as SR_PREFIX};

use jwt::algorithm::{AlgorithmType, SigningAlgorithm, VerifyingAlgorithm};

use anyhow::Result;

use subxt::utils::AccountId32;
use tfchain_client::client::KeyPair;
pub const SIGNATURE_LENGTH: usize = 65;

pub trait Identity: Clone + Send + Sync {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> Result<()>;
    fn account(&self) -> AccountId32;
}

pub trait Signer: Identity {
    /// sign a message. the returned signature is a 64 bytes signature prefixed with
    /// one byte which indicates the type of the key.
    fn sign<M: AsRef<[u8]>>(&self, message: M) -> [u8; SIGNATURE_LENGTH];
    fn pair(&self) -> KeyPair;
}

pub fn validate_signature_len<S: AsRef<[u8]>>(sig: &S) -> Result<()> {
    let sig = sig.as_ref();
    if sig.len() != SIGNATURE_LENGTH {
        bail!(
            "invalid signature length, expected: {}, found: {}",
            SIGNATURE_LENGTH,
            sig.len()
        );
    }

    Ok(())
}

// Implement identity for AccountId32
impl Identity for AccountId32 {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> Result<()> {
        // this is a tricky one because we don't know what kind of `key` the <account id 32> is
        // so we have to know that from the signature itself. We will assume that signers will always
        // prefix the signature with their type.
        // so instead of the standard 64 bytes signatures, we will have a signature of 65 bytes length
        validate_signature_len(&sig)?;

        let sig = sig.as_ref();
        match sig[0] {
            ED_PREFIX => {
                let pk = Ed25519Identity::from(self);
                pk.verify(sig, message)
            }
            SR_PREFIX => {
                let pk = Sr25519Identity::from(self);
                pk.verify(sig, message)
            }
            _ => Err(anyhow!(
                "invalid signature type (expected either sr25519 or ed25519)"
            )),
        }
    }

    fn account(&self) -> AccountId32 {
        self.clone()
    }
}

pub struct JwtSigner<'a, S: Signer>(&'a S);
impl<'a, S> From<&'a S> for JwtSigner<'a, S>
where
    S: Signer,
{
    fn from(value: &'a S) -> Self {
        Self(value)
    }
}

impl<'a, S> SigningAlgorithm for JwtSigner<'a, S>
where
    S: Signer,
{
    fn algorithm_type(&self) -> AlgorithmType {
        // we return ALWAYS a fixed
        // type.
        AlgorithmType::Rs512
    }

    fn sign(&self, header: &str, claims: &str) -> Result<String, jwt::Error> {
        use base64::{CharacterSet, Config};
        let data = format!("{}.{}", header, claims);
        let signature = self.0.sign(data);
        let cfg = Config::new(CharacterSet::UrlSafe, false);
        Ok(base64::encode_config(signature, cfg))
    }
}

pub struct JwtVerifier<'a, I: Identity>(&'a I);
impl<'a, I> From<&'a I> for JwtVerifier<'a, I>
where
    I: Identity,
{
    fn from(value: &'a I) -> Self {
        Self(value)
    }
}

impl<'a, I> VerifyingAlgorithm for JwtVerifier<'a, I>
where
    I: Identity,
{
    fn algorithm_type(&self) -> AlgorithmType {
        AlgorithmType::Rs512
    }

    fn verify_bytes(
        &self,
        header: &str,
        claims: &str,
        signature: &[u8],
    ) -> Result<bool, jwt::Error> {
        let data = format!("{}.{}", header, claims);
        self.0
            .verify(signature, data)
            .map(|_| true)
            .map_err(|_| jwt::Error::InvalidSignature)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum Signers {
    Ed25519(Ed25519Signer),
    Sr25519(Sr25519Signer),
}

impl Identity for Signers {
    fn verify<S: AsRef<[u8]>, M: AsRef<[u8]>>(&self, sig: S, message: M) -> Result<()> {
        match self {
            Signers::Ed25519(ref sk) => sk.verify(sig, message),
            Signers::Sr25519(ref sk) => sk.verify(sig, message),
        }
    }

    fn account(&self) -> AccountId32 {
        match self {
            Signers::Ed25519(ref sk) => sk.account(),
            Signers::Sr25519(ref sk) => sk.account(),
        }
    }
}

impl Signer for Signers {
    fn sign<M: AsRef<[u8]>>(&self, message: M) -> [u8; SIGNATURE_LENGTH] {
        match self {
            Signers::Ed25519(ref sk) => sk.sign(message),
            Signers::Sr25519(ref sk) => sk.sign(message),
        }
    }

    fn pair(&self) -> KeyPair {
        match self {
            Signers::Ed25519(ref sk) => sk.pair(),
            Signers::Sr25519(ref sk) => sk.pair(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum KeyType {
    Ed25519,
    Sr25519,
}

impl FromStr for KeyType {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ed25519" => Ok(KeyType::Ed25519),
            "sr25519" => Ok(KeyType::Sr25519),
            _ => Err("invalid key type"),
        }
    }
}

impl Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            KeyType::Ed25519 => "ed25519",
            KeyType::Sr25519 => "sr25519",
        };

        f.write_str(s)
    }
}
