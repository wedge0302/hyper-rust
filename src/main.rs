use std::sync::Arc;

use leaglag::{HyperliquidTrader, SharedError};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), SharedError> {
    let simbols = leaglag::get_simbols()?;
    leaglag::write_simbols_file(&simbols, "simbols.json")?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let trader = Arc::new(HyperliquidTrader::from_default_paths().await?);
        leaglag::stream_binance_signals(&simbols.simbols, trader).await
    })?;
    Ok(())
}
