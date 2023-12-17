use aes_gcm::{aead::Aead, Aes256Gcm, KeyInit, Nonce};
use bip39::{Language, Mnemonic};
use rand_core::{OsRng, RngCore};
use secp256k1::constants;
use secp256k1::{KeyPair, PublicKey, Secp256k1};
use std::str::FromStr;

pub const PUBLIC_KEY_SIZE: usize = constants::PUBLIC_KEY_SIZE;
pub const SHARED_KEY_SIZE: usize = 32;
pub const NONCE_KEY_SIZE: usize = 12;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid bip39 phrase")]
    InvalidPhrase,
    #[error("invalid entropy")]
    InvalidEntropy,
    #[error("invalid seed")]
    InvalidSeed,
    #[error("invalid cipher data")]
    InvalidCipher,
    #[error("k256: {0}")]
    Secp256(#[from] secp256k1::Error),
}

/// Pair is a secure key (pair) that is used for encryption
#[derive(Clone)]
pub struct Pair(KeyPair);

impl FromStr for Pair {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let secp = Secp256k1::new();
        let kp: KeyPair = match s.strip_prefix("0x") {
            None => {
                // no prefix, we assume this is a bip39 Mnemonic
                let mnemonic = Mnemonic::parse_in_normalized(Language::English, s)
                    .map_err(|_| Error::InvalidPhrase)?;
                let (entropy, entropy_len) = mnemonic.to_entropy_array();
                let seed = substrate_bip39::seed_from_entropy(&entropy[0..entropy_len], "")
                    .map_err(|_| Error::InvalidEntropy)?;
                KeyPair::from_seckey_slice(&secp, &seed[..32])?
            }
            Some(h) => {
                let seed = hex::decode(h).map_err(|_| Error::InvalidSeed)?;
                KeyPair::from_seckey_slice(&secp, &seed)?
            }
        };

        Ok(Self(kp))
    }
}

impl Pair {
    /// get public key of the key pair. it's okay to share this public key
    /// with other peers. actually it's the only way to use this for encryption
    /// is by making your public key accessable for other peers
    pub fn public(&self) -> [u8; PUBLIC_KEY_SIZE] {
        self.0.public_key().serialize()
    }

    /// compute a "shared" key (used for encryption) knowing the public key of the other peer
    /// Two peers A, and B can agree on the same shared key by sharing their own public key
    /// hence
    ///  shared1 = A.sk.shared(B.pk)
    ///  shared2 = B.sk.shared(A.pk)
    ///  assert_eq(shared1, shared2) <- is valid
    pub(crate) fn shared<K: AsRef<[u8]>>(&self, pk: K) -> Result<[u8; SHARED_KEY_SIZE], Error> {
        let pk = PublicKey::from_slice(pk.as_ref())?;

        // we take the x coordinate of the secret point.
        let point = &secp256k1::ecdh::shared_secret_point(&pk, &self.0.secret_key())[..32];
        use sha2::{Digest, Sha256};

        let mut sh = Sha256::new();
        sh.update(point);
        Ok(sh.finalize().into())
    }

    /// Given two peers A, and B. Peer A can encrypt the data in a way that only B can decrypt it
    /// by driving a shared key (check Pair::shared)
    pub fn encrypt<K: AsRef<[u8]>, D: AsRef<[u8]>>(
        &self,
        pk: K,
        data: D,
    ) -> Result<Vec<u8>, Error> {
        let shared = self.shared(&pk)?;
        let mut nonce = [0u8; NONCE_KEY_SIZE];
        OsRng.fill_bytes(&mut nonce);

        let aes = Aes256Gcm::new_from_slice(&shared).unwrap();

        let nonce = Nonce::from(nonce);
        let encrypted = aes
            .encrypt(&nonce, data.as_ref())
            .map_err(|_| Error::InvalidCipher)?;

        let mut payload = Vec::with_capacity(nonce.len() + encrypted.len());
        payload.extend(nonce);
        payload.extend(encrypted);

        Ok(payload)
    }

    /// Given two peers A, and B. If peer A encrypted data with (A.sk + B.pk)
    /// this method is to allow peer B to decrypt the data with (B.sk + A.pk)
    pub fn decrypt<K: AsRef<[u8]>, D: AsRef<[u8]>>(
        &self,
        pk: K,
        data: D,
    ) -> Result<Vec<u8>, Error> {
        let data = data.as_ref();
        if data.len() < NONCE_KEY_SIZE {
            return Err(Error::InvalidCipher);
        }

        let shared = self.shared(&pk)?;
        let nonce = Nonce::from_slice(&data[..NONCE_KEY_SIZE]);

        let aes = Aes256Gcm::new_from_slice(&shared).unwrap();

        let decrypted = aes
            .decrypt(nonce, &data[NONCE_KEY_SIZE..])
            .map_err(|_| Error::InvalidCipher)?;
        Ok(decrypted)
    }
}

#[cfg(test)]
mod test {
    use super::Pair;

    #[test]
    fn test_shared_key() {
        let sk1: Pair = "0x340f7341b312bcc61aeea7a76d759d809b8601cbc6d22cb2817278e346137a5d"
            .parse()
            .unwrap();

        let sk2: Pair =
            "tackle blouse grain dawn adult loyal tattoo price access tilt chimney silk"
                .parse()
                .unwrap();

        let shared1 = sk1.shared(&sk2.public()).unwrap();
        let shared2 = sk2.shared(&sk1.public()).unwrap();

        assert_eq!(shared1, shared2);

        let encrypted = sk1.encrypt(sk2.public(), "hello world").unwrap();
        let decrypted = sk2.decrypt(sk1.public(), encrypted).unwrap();

        assert_eq!("hello world", String::from_utf8(decrypted).unwrap());
    }
}
