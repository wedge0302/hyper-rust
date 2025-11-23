mod binance;
mod config;
mod trading;

pub use binance::stream_binance_signals;
pub use config::{config_error, HyperliquidCredentials};
pub use trading::{AccountMonitor, HyperliquidTrader};

pub type SharedError = Box<dyn std::error::Error + Send + Sync>;

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

const INFO_ENDPOINT: &str = "https://api.hyperliquid.xyz/info";
const MIN_DAY_VOLUME_USD: f64 = 200_000.0;

/// 返回 Hyperliquid 当前可交易的币种列表。
pub fn get_simbols() -> Result<SimbolsResponse, SharedError> {
    let mut tradable = fetch_tradable_contracts()?;
    tradable.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(SimbolsResponse {
        simbols: tradable.into_iter().map(|contract| contract.name).collect(),
    })
}

/// 将符号列表写入到 `simbols.json`。
pub fn write_simbols_file(
    response: &SimbolsResponse,
    output_path: impl AsRef<Path>,
) -> Result<(), SharedError> {
    let json = serde_json::to_string_pretty(response)?;
    fs::write(output_path, json)?;
    Ok(())
}

fn fetch_tradable_contracts() -> Result<Vec<ContractInfo>, SharedError> {
    let client = Client::new();
    let payload = MetaRequest { req_type: "metaAndAssetCtxs" };
    let data: Vec<MetaOrContext> = client
        .post(INFO_ENDPOINT)
        .json(&payload)
        .send()?
        .error_for_status()?
        .json()?;

    let mut universe: Option<Vec<ContractInfo>> = None;
    let mut contexts: Option<Vec<AssetContextInfo>> = None;

    for item in data {
        match item {
            MetaOrContext::Meta(meta) => universe = Some(meta.universe),
            MetaOrContext::Context(ctx) => contexts = Some(ctx),
        }
    }

    let universe = universe.ok_or_else(|| config_error("meta response missing universe"))?;
    let contexts = contexts.unwrap_or_default();

    let filtered = universe
        .into_iter()
        .zip(contexts.into_iter())
        .filter(|(contract, ctx)| {
            if contract.is_delisted {
                return false;
            }
            ctx.day_ntl_vlm_value().map_or(false, |vol| vol >= MIN_DAY_VOLUME_USD)
        })
        .map(|(contract, _)| contract)
        .collect();
    Ok(filtered)
}

#[derive(Serialize, Deserialize)]
pub struct SimbolsResponse {
    pub simbols: Vec<String>,
}

#[derive(Serialize)]
struct MetaRequest<'a> {
    #[serde(rename = "type")]
    req_type: &'a str,
}

#[derive(Deserialize)]
struct ContractInfo {
    name: String,
    #[serde(rename = "isDelisted", default)]
    is_delisted: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MetaOrContext {
    Meta(MetaContainer),
    Context(Vec<AssetContextInfo>),
}

#[derive(Deserialize)]
struct MetaContainer {
    universe: Vec<ContractInfo>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct AssetContextInfo {
    #[serde(rename = "dayNtlVlm", default)]
    day_ntl_vlm: String,
}

impl AssetContextInfo {
    fn day_ntl_vlm_value(&self) -> Option<f64> {
        self.day_ntl_vlm.parse::<f64>().ok()
    }
}
