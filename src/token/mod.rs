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
}

impl<S> TokenBuilder<S>
where
    S: Signer,
{
    pub fn new(id: u32, signer: S) -> Self {
        Self { id, signer }
    }

    pub fn token(&self, age: u64) -> Result<String, Error> {
        token(&self.signer, self.id, age)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    #[serde(rename = "sub")]
    pub id: u32,
    #[serde(rename = "iat")]
    pub timestamp: u64,
    #[serde(rename = "exp")]
    pub age: u64,
}

fn now() -> Result<u64, Error> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| Error::InvalidTime)?
        .as_secs())
}

/// create a new jwt token given the signer, id of the twin, and expiration (age) in seconds
pub fn token<S: Signer>(signer: &S, id: u32, age: u64) -> Result<String, Error> {
    // get timestamp
    let now = now()?;

    let signer: JwtSigner<_> = signer.into();
    let claims = Claims {
        id: id,
        timestamp: now,
        age,
    };

    claims.sign_with_key(&signer).map_err(Error::Jwt)
}

impl FromStr for Claims {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let token: Token<Header, Claims, _> = Token::parse_unverified(s)?;

        let claims = token.claims();
        let now = now()?;
        if claims.timestamp + claims.age < now {
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
    use crate::identity::Sr25519Signer;

    #[test]
    fn create_token() {
        let signer = Sr25519Signer::try_from("//Alice").unwrap();
        //let identity = signer.account();

        let token = super::token(&signer, 100, 20).unwrap();

        let claims: super::Claims = token.parse().unwrap();

        assert_eq!(claims.id, 100);
        assert_eq!(claims.age, 20);
    }
}