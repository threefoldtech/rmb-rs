use jwt::{Error as JwtError, Header, SignWithKey, Token, VerifyWithKey};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::SystemTime};

use crate::identity::{Identity, JwtSigner, JwtVerifier, Signer};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("jwt: {0}")]
    Jwt(#[from] JwtError),

    #[error("jwt has expired")]
    Expired,

    #[error("system time is invalid")]
    InvalidTime,
}

/// Token builder is a wrapper around the token() function
/// to conveniently create a new token
#[derive(Clone)]
pub struct TokenBuilder<S: Signer> {
    id: u32,
    signer: S,
    sid: Option<String>,
}

impl<S> TokenBuilder<S>
where
    S: Signer,
{
    /// create a new token build with this twin `id` and optional `sid` (session id)
    /// a peer can maintain only 1 connection per (id, sid). An rmb-peer always use
    /// sid = None, hence you cannot run 2 instance of rmb-peer with the same identity
    /// other connections for the same twin has to have their own session id.
    ///
    /// a signer is used to sign the jwt token for validation.
    pub fn new(id: u32, sid: Option<String>, signer: S) -> Self {
        Self { id, signer, sid }
    }

    /// get the token as string.
    pub fn token(&self, ttl: u64) -> Result<String, Error> {
        token(&self.signer, self.id, self.sid.clone(), ttl)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    #[serde(rename = "sub")]
    pub id: u32,
    #[serde(rename = "sid")]
    pub sid: Option<String>,
    #[serde(rename = "iat")]
    pub timestamp: u64,
    #[serde(rename = "exp")]
    pub expiration: u64,
}

fn now() -> Result<u64, Error> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| Error::InvalidTime)?
        .as_secs())
}

/// create a new jwt token given the signer, id of the twin, and expiration (age) in seconds
pub fn token<S: Signer>(
    signer: &S,
    id: u32,
    sid: Option<String>,
    ttl: u64,
) -> Result<String, Error> {
    // get timestamp
    let now = now()?;

    let signer: JwtSigner<_> = signer.into();
    let claims = Claims {
        id,
        sid,
        timestamp: now,
        expiration: now + ttl,
    };

    claims.sign_with_key(&signer).map_err(Error::Jwt)
}

impl FromStr for Claims {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let token: Token<Header, Claims, _> = Token::parse_unverified(s)?;

        let claims = token.claims();
        let now = now()?;
        if now > claims.expiration {
            return Err(Error::Expired);
        }

        Ok(claims.clone())
    }
}

pub fn verify<I, J>(identity: &I, jwt: J) -> Result<(), Error>
where
    I: Identity,
    J: AsRef<str>,
{
    let verifier: JwtVerifier<_> = identity.into();

    let _: Token<Header, Claims, _> = jwt.as_ref().verify_with_key(&verifier)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::identity::{Ed25519Signer, Sr25519Signer};

    #[test]
    fn create_token_sr() {
        let signer = Sr25519Signer::try_from("//Alice").unwrap();
        //let identity = signer.account();

        let token = super::token(&signer, 100, None, 20).unwrap();

        let claims: super::Claims = token.parse().unwrap();

        assert_eq!(claims.id, 100);
        assert_eq!(claims.expiration, claims.timestamp + 20);
    }

    #[test]
    fn create_token_ed() {
        let signer = Ed25519Signer::try_from("//Alice").unwrap();
        //let identity = signer.account();

        let token = super::token(&signer, 100, None, 20).unwrap();

        let claims: super::Claims = token.parse().unwrap();

        assert_eq!(claims.id, 100);
        assert_eq!(claims.expiration, claims.timestamp + 20);
    }

    #[test]
    fn load_token() {
        let t = "eyJhbGciOiJSUzUxMiJ9.eyJzdWIiOjcsImlhdCI6MTY3MzI2NzE1MCwiZXhwIjo2MH0.ZRCH2iy6VnwUFfhL0h7tjlweqJT9tQJuHpApPFD0tDDObyBdGUBrqy2EXS8XSX7YQUH1hswylONxoNLvFkdb2AY";

        let token: Token<Header, Claims, _> = Token::parse_unverified(t).unwrap();

        println!("headers: {:#?}", token.header());
        println!("{:#?}", token.claims());
    }
}
