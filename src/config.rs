use crate::SharedError;
use ethers_core::types::H160;
use ethers_signers::LocalWallet;
use serde::Deserialize;
use std::{fs, io, path::Path, str::FromStr};

#[derive(Deserialize)]
struct RawHyperliquidConfig {
    address: String,
    api_key: String,
    secret_key: String,
}

pub fn config_error(msg: impl Into<String>) -> SharedError {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, msg.into()))
}

#[derive(Clone, Debug)]
pub struct HyperliquidCredentials {
    address: H160,
    api_key: String,
    secret_key: String,
}

impl HyperliquidCredentials {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, SharedError> {
        let config_text = fs::read_to_string(path)?;
        let raw: RawHyperliquidConfig = serde_json::from_str(&config_text)?;
        let address = H160::from_str(raw.address.trim())
            .map_err(|err| config_error(format!("invalid address in config: {err}")))?;
        let secret_key = raw.secret_key.trim().trim_start_matches("0x").to_string();
        Ok(Self {
            address,
            api_key: raw.api_key,
            secret_key,
        })
    }

    pub fn wallet(&self) -> Result<LocalWallet, SharedError> {
        LocalWallet::from_str(&self.secret_key)
            .map_err(|err| config_error(format!("invalid secret key: {err}")))
    }

    pub fn address(&self) -> H160 {
        self.address
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }
}
