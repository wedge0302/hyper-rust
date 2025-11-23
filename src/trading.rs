use crate::{
    config::{config_error, HyperliquidCredentials},
    SharedError,
};
use std::collections::{HashMap, HashSet, VecDeque};
use ethers_core::types::H160;
use hyperliquid_rust_sdk::{
    AssetPosition, BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest,
    ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus, InfoClient, MarketOrderParams,
    Message, Subscription, UserStateResponse,
};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    sync::{Mutex, RwLock},
    task::JoinHandle,
    time::{interval, sleep, MissedTickBehavior},
};

const ORDER_NOTIONAL_USD: f64 = 12.0;
const ORDER_CANCEL_DELAY_MS: u64 = 2_000;
const TAKE_PROFIT_RETURN: f64 = 0.025;
const STOP_LOSS_RETURN: f64 = 0.005;
const TRAILING_STOP_THRESHOLD: f64 = 0.002;
const TRAILING_DRAWDOWN_RATIO: f64 = 0.33;
const CLOSE_ORDER_ATTEMPTS: usize = 3;
const CLOSE_ORDER_TIMEOUT_SECS: u64 = 10;
const POSITION_POLL_INTERVAL_SECS: u64 = 3;
const PRICE_RESET_EPSILON: f64 = 1e-9;
const MIN_POSITION_SIZE: f64 = 1e-8;
const MARGIN_USAGE_LIMIT: f64 = 0.8;
const LOSS_THRESHOLD: f64 = 0.05;
const LOSS_WINDOW_SECS: u64 = 4 * 60 * 60;
const LOSS_COOLDOWN_SECS: u64 = 60 * 60;

pub type SignalSender = UnboundedSender<TradeSignal>;
pub type SignalReceiver = UnboundedReceiver<TradeSignal>;

#[derive(Clone, Debug)]
pub struct TradeSignal {
    pub asset: String,
    pub binance_symbol: String,
    pub is_buy: bool,
    pub amplitude: f64,
}

impl TradeSignal {
    pub fn asset(&self) -> &str {
        &self.asset
    }

    pub fn binance_symbol(&self) -> &str {
        &self.binance_symbol
    }
}

pub struct HyperliquidTrader {
    exchange: ExchangeClient,
    info_client: InfoClient,
    ws_info_client: Mutex<InfoClient>,
    base_url: BaseUrl,
    credentials: HyperliquidCredentials,
    asset_precisions: HashMap<String, u32>,
    asset_indices: HashMap<String, u32>,
    bbo_quotes: Arc<RwLock<HashMap<String, BboQuote>>>,
    bbo_subscriptions: Mutex<HashSet<String>>,
}

impl HyperliquidTrader {
    pub async fn from_config(
        path: impl AsRef<Path>,
        base_url: Option<BaseUrl>,
    ) -> Result<Self, SharedError> {
        let credentials = HyperliquidCredentials::from_file(path)?;
        Self::new(credentials, base_url).await
    }

    pub async fn new(
        credentials: HyperliquidCredentials,
        base_url: Option<BaseUrl>,
    ) -> Result<Self, SharedError> {
        let wallet = credentials.wallet()?;
        let base_url = base_url.unwrap_or(BaseUrl::Mainnet);
        let exchange = ExchangeClient::new(None, wallet, Some(base_url), None, None).await?;
        let info_client = InfoClient::new(None, Some(base_url)).await?;
        let ws_info_client = InfoClient::with_reconnect(None, Some(base_url)).await?;
        let asset_precisions = exchange
            .meta
            .universe
            .iter()
            .map(|meta| (meta.name.clone(), meta.sz_decimals))
            .collect();
        let asset_indices = exchange.coin_to_asset.clone();
        Ok(Self {
            exchange,
            info_client,
            ws_info_client: Mutex::new(ws_info_client),
            base_url,
            credentials,
            asset_precisions,
            asset_indices,
            bbo_quotes: Arc::new(RwLock::new(HashMap::new())),
            bbo_subscriptions: Mutex::new(HashSet::new()),
        })
    }

    pub async fn from_default_paths() -> Result<Self, SharedError> {
        const CANDIDATES: [&str; 3] = ["config.json", "../config.json", "../../config.json"];
        for candidate in CANDIDATES {
            let path = Path::new(candidate);
            if path.exists() {
                return Self::from_config(path, None).await;
            }
        }
        Err(config_error("未找到 config.json，请将其放在当前或上级目录中"))
    }

    pub fn base_url(&self) -> BaseUrl {
        self.base_url
    }

    pub async fn place_limit_order(
        &self,
        asset: &str,
        is_buy: bool,
        size: f64,
        limit_price: f64,
        tif: &str,
        reduce_only: bool,
    ) -> Result<Vec<ExchangeDataStatus>, SharedError> {
        let order = ClientOrderRequest {
            asset: asset.to_string(),
            is_buy,
            reduce_only,
            limit_px: limit_price,
            sz: size,
            cloid: None,
            order_type: ClientOrder::Limit(ClientLimit {
                tif: tif.to_string(),
            }),
        };
        let response = self.exchange.order(order, None).await?;
        unwrap_exchange_statuses(response)
    }

    pub async fn place_market_order(
        &self,
        asset: &str,
        is_buy: bool,
        size: f64,
        slippage: Option<f64>,
    ) -> Result<Vec<ExchangeDataStatus>, SharedError> {
        let params = MarketOrderParams {
            asset,
            is_buy,
            sz: size,
            px: None,
            slippage,
            cloid: None,
            wallet: None,
        };
        let response = self.exchange.market_open(params).await?;
        unwrap_exchange_statuses(response)
    }

    pub async fn cancel_order(
        &self,
        asset: &str,
        oid: u64,
    ) -> Result<Vec<ExchangeDataStatus>, SharedError> {
        let request = ClientCancelRequest {
            asset: asset.to_string(),
            oid,
        };
        let response = self.exchange.cancel(request, None).await?;
        unwrap_exchange_statuses(response)
    }

    pub async fn monitor_account(&self) -> Result<AccountMonitor, SharedError> {
        AccountMonitor::new(self.credentials.address(), Some(self.base_url)).await
    }

    pub fn round_order_size(
        &self,
        asset: &str,
        size: f64,
    ) -> Result<f64, SharedError> {
        let decimals = self
            .asset_precisions
            .get(asset)
            .ok_or_else(|| config_error(format!("无法在 Meta 中找到 {asset} 的精度信息")))?;
        let rounded = round_to_decimals(size, *decimals);
        if rounded <= 0.0 {
            return Err(config_error(format!("{asset} 的计算后下单数量无效: {rounded}")));
        }
        Ok(rounded)
    }

    pub fn round_limit_price(
        &self,
        asset: &str,
        price: f64,
    ) -> Result<f64, SharedError> {
        let &asset_index = self
            .asset_indices
            .get(asset)
            .ok_or_else(|| config_error(format!("{asset} 不在交易对映射中")))?;
        let sz_decimals = self
            .asset_precisions
            .get(asset)
            .ok_or_else(|| config_error(format!("缺少 {asset} 的价格精度信息")))?;
        let max_decimals: u32 = if asset_index < 10_000 { 6 } else { 8 };
        let price_decimals = max_decimals.saturating_sub(*sz_decimals);
        let rounded = round_to_significant_and_decimal(price, 5, price_decimals);
        if rounded <= 0.0 {
            return Err(config_error(format!("{asset} 的价格无效: {rounded}")));
        }
        Ok(rounded)
    }

    async fn ensure_bbo_subscription(&self, asset: &str) -> Result<(), SharedError> {
        {
            let subs = self.bbo_subscriptions.lock().await;
            if subs.contains(asset) {
                return Ok(());
            }
        }

        let asset_name = asset.to_string();
        let (sender, mut receiver) = unbounded_channel();
        {
            let mut client = self.ws_info_client.lock().await;
            client
                .subscribe(Subscription::L2Book { coin: asset_name.clone() }, sender)
                .await?;
        }

        {
            let mut subs = self.bbo_subscriptions.lock().await;
            subs.insert(asset_name.clone());
        }

        let quotes = Arc::clone(&self.bbo_quotes);
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                if let Message::L2Book(book) = message {
                    let bid = book
                        .data
                        .levels
                        .get(0)
                        .and_then(|bids| bids.first())
                        .and_then(|lvl| lvl.px.parse::<f64>().ok());
                    let ask = book
                        .data
                        .levels
                        .get(1)
                        .and_then(|asks| asks.first())
                        .and_then(|lvl| lvl.px.parse::<f64>().ok());
                    if bid.is_none() && ask.is_none() {
                        continue;
                    }
                    let mut guard = quotes.write().await;
                    guard.insert(
                        book.data.coin.clone(),
                        BboQuote {
                            best_bid: bid,
                            best_ask: ask,
                            _last_update_ms: book.data.time,
                            _last_refresh: Instant::now(),
                        },
                    );
                }
            }
        });

        Ok(())
    }

    async fn current_bbo_price(&self, asset: &str, is_buy: bool) -> Option<f64> {
        let guard = self.bbo_quotes.read().await;
        guard.get(asset).and_then(|quote| quote.best_price(is_buy))
    }

    pub async fn best_opposite_price(
        &self,
        asset: &str,
        is_buy: bool,
    ) -> Result<Option<f64>, SharedError> {
        self.ensure_bbo_subscription(asset).await?;

        if let Some(price) = self.current_bbo_price(asset, is_buy).await {
            return Ok(Some(price));
        }

        let snapshot = self
            .info_client
            .l2_snapshot(asset.to_string())
            .await
            .map_err(|err| config_error(format!("获取 {asset} orderbook 失败: {err}")))?;

        let fallback_bid = snapshot
            .levels
            .get(0)
            .and_then(|bids| bids.first())
            .and_then(|level| level.px.parse::<f64>().ok());
        let fallback_ask = snapshot
            .levels
            .get(1)
            .and_then(|asks| asks.first())
            .and_then(|level| level.px.parse::<f64>().ok());
        let fallback = if is_buy { fallback_ask } else { fallback_bid };
        if fallback.is_some() {
            let mut guard = self.bbo_quotes.write().await;
            guard.insert(
                asset.to_string(),
                BboQuote {
                    best_bid: fallback_bid,
                    best_ask: fallback_ask,
                    _last_update_ms: snapshot.time,
                    _last_refresh: Instant::now(),
                },
            );
        }

        Ok(fallback)
    }

    pub async fn user_state(&self) -> Result<UserStateResponse, SharedError> {
        self.info_client
            .user_state(self.credentials.address())
            .await
            .map_err(|err| config_error(format!("获取账户仓位失败: {err}")))
    }
}

pub struct AccountMonitor {
    info_client: InfoClient,
    subscriptions: Vec<u32>,
    receiver: UnboundedReceiver<Message>,
    address: H160,
    base_url: BaseUrl,
}

impl AccountMonitor {
    pub async fn new(
        address: H160,
        base_url: Option<BaseUrl>,
    ) -> Result<Self, SharedError> {
        let base_url = base_url.unwrap_or(BaseUrl::Mainnet);
        let mut info_client = InfoClient::with_reconnect(None, Some(base_url)).await?;
        let (sender, receiver) = unbounded_channel();
        let mut subscriptions = Vec::new();

        subscriptions.push(
            info_client
                .subscribe(Subscription::UserEvents { user: address }, sender.clone())
                .await?,
        );
        subscriptions.push(
            info_client
                .subscribe(Subscription::OrderUpdates { user: address }, sender.clone())
                .await?,
        );
        subscriptions.push(
            info_client
                .subscribe(
                    Subscription::UserNonFundingLedgerUpdates { user: address },
                    sender,
                )
                .await?,
        );

        Ok(Self {
            info_client,
            subscriptions,
            receiver,
            address,
            base_url,
        })
    }

    pub fn address(&self) -> H160 {
        self.address
    }

    pub fn base_url(&self) -> BaseUrl {
        self.base_url
    }

    pub async fn next_event(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }

    pub fn receiver(&mut self) -> &mut UnboundedReceiver<Message> {
        &mut self.receiver
    }

    pub async fn stop(&mut self) -> Result<(), SharedError> {
        for subscription in &self.subscriptions {
            if let Err(err) = self.info_client.unsubscribe(*subscription).await {
                eprintln!("failed to unsubscribe from Hyperliquid feed {subscription}: {err}");
            }
        }
        self.subscriptions.clear();
        Ok(())
    }
}

pub struct RiskController {
    state: RwLock<RiskState>,
}

struct RiskState {
    margin_ratio: f64,
    account_value_history: VecDeque<(Instant, f64)>,
    pause_until: Option<Instant>,
}

impl RiskController {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(RiskState {
                margin_ratio: 0.0,
                account_value_history: VecDeque::new(),
                pause_until: None,
            }),
        }
    }

    pub async fn update_from_snapshot(&self, account_value: f64, margin_used: f64) {
        let mut state = self.state.write().await;
        state.margin_ratio = if account_value > 0.0 {
            (margin_used / account_value).max(0.0)
        } else {
            1.0
        };
        let now = Instant::now();
        state.account_value_history.push_back((now, account_value));
        while let Some((ts, _)) = state.account_value_history.front() {
            if now.duration_since(*ts) > Duration::from_secs(LOSS_WINDOW_SECS) {
                state.account_value_history.pop_front();
            } else {
                break;
            }
        }
        if let Some((_, oldest_value)) = state.account_value_history.front().copied() {
            if oldest_value > 0.0 && account_value <= oldest_value * (1.0 - LOSS_THRESHOLD) {
                let new_until = now + Duration::from_secs(LOSS_COOLDOWN_SECS);
                state.pause_until = Some(match state.pause_until {
                    Some(existing) => existing.max(new_until),
                    None => new_until,
                });
                let drop_pct = ((oldest_value - account_value) / oldest_value) * 100.0;
                println!(
                    "账户在最近 4 小时回撤 {:.2}%，风控将暂停信号下单 60 分钟。",
                    drop_pct
                );
            }
        }
    }

    pub(crate) async fn check_entry_permitted(&self) -> Result<(), RiskBlockReason> {
        let state = self.state.read().await;
        if state.margin_ratio >= MARGIN_USAGE_LIMIT {
            return Err(RiskBlockReason::MarginExceeded {
                ratio: state.margin_ratio,
            });
        }
        if let Some(until) = state.pause_until {
            let now = Instant::now();
            if let Some(remaining) = until.checked_duration_since(now) {
                if remaining > Duration::from_secs(0) {
                    return Err(RiskBlockReason::CooldownActive { remaining });
                }
            }
        }
        Ok(())
    }
}

pub(crate) enum RiskBlockReason {
    MarginExceeded { ratio: f64 },
    CooldownActive { remaining: Duration },
}

impl RiskBlockReason {
    fn message(&self) -> String {
        match self {
            RiskBlockReason::MarginExceeded { ratio } => format!(
                "保证金使用率 {:.2}% 超过 {:.0}% 阈值",
                ratio * 100.0,
                MARGIN_USAGE_LIMIT * 100.0
            ),
            RiskBlockReason::CooldownActive { remaining } => {
                let secs = remaining.as_secs();
                let mins = secs / 60;
                if mins > 0 {
                    format!("收益回撤风控激活，{} 分 {} 秒后恢复", mins, secs % 60)
                } else {
                    format!("收益回撤风控激活，{} 秒后恢复", secs)
                }
            }
        }
    }
}

pub struct PositionMonitor {
    trader: Arc<HyperliquidTrader>,
    risk: Arc<RiskController>,
    tracked: Mutex<HashMap<String, PositionStats>>,
}

impl PositionMonitor {
    pub fn spawn(trader: Arc<HyperliquidTrader>, risk: Arc<RiskController>) -> Arc<Self> {
        let monitor = Arc::new(Self {
            trader,
            risk,
            tracked: Mutex::new(HashMap::new()),
        });
        let runner = Arc::clone(&monitor);
        tokio::spawn(async move {
            runner.run().await;
        });
        monitor
    }

    async fn run(self: Arc<Self>) {
        Self::poll_once(&self).await;
        let mut ticker = interval(Duration::from_secs(POSITION_POLL_INTERVAL_SECS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            Self::poll_once(&self).await;
        }
    }

    async fn poll_once(this: &Arc<Self>) {
        match this.trader.user_state().await {
            Ok(snapshot) => Self::handle_snapshot(this, snapshot).await,
            Err(err) => eprintln!("获取 Hyperliquid 账户状态失败: {err}"),
        }
    }

    async fn handle_snapshot(this: &Arc<Self>, snapshot: UserStateResponse) {
        if let (Some(account_value), Some(margin_used)) = (
            parse_decimal(&snapshot.margin_summary.account_value),
            parse_decimal(&snapshot.margin_summary.total_margin_used),
        ) {
            this.risk
                .update_from_snapshot(account_value, margin_used)
                .await;
        }

        let mut active_assets = HashSet::new();
        let mut closings = Vec::new();

        for position in snapshot
            .asset_positions
            .into_iter()
            .filter_map(parse_active_position)
        {
            active_assets.insert(position.asset.clone());
            let latest_price = match this
                .trader
                .best_opposite_price(&position.asset, !position.is_long)
                .await
            {
                Ok(Some(price)) => price,
                Ok(None) => {
                    eprintln!(
                        "[{}] 无法获取最新对手价，暂不执行移动止盈/止损。",
                        position.asset
                    );
                    continue;
                }
                Err(err) => {
                    eprintln!("[{}] 获取对手价失败: {err}", position.asset);
                    continue;
                }
            };
            let pnl = position.return_from_price(latest_price);
            if this.evaluate_position(&position, pnl).await {
                closings.push(position.clone());
            }
        }

        this.prune_positions(&active_assets).await;

        for position in closings {
            let monitor = Arc::clone(this);
            tokio::spawn(async move {
                let asset_name = position.asset.clone();
                let closer = Arc::clone(&monitor);
                match closer.force_close_position(position).await {
                    Ok(_) => monitor.finish_closing(&asset_name, true).await,
                    Err(err) => {
                        eprintln!("[{asset_name}] 平仓失败: {err}");
                        monitor.finish_closing(&asset_name, false).await;
                    }
                }
            });
        }
    }

    async fn prune_positions(&self, active_assets: &HashSet<String>) {
        let mut tracked = self.tracked.lock().await;
        tracked.retain(|asset, _| active_assets.contains(asset));
    }

    async fn evaluate_position(
        &self,
        position: &ActivePosition,
        current_return: f64,
    ) -> bool {
        let mut tracked = self.tracked.lock().await;
        let stats = tracked
            .entry(position.asset.clone())
            .or_insert_with(|| PositionStats::new(position.entry_price, position.is_long, current_return));
        if stats.needs_reset(position.entry_price, position.is_long) {
            stats.reset(position.entry_price, position.is_long, current_return);
        }
        if current_return > stats.peak_return {
            stats.peak_return = current_return;
        }
        if stats.closing {
            return false;
        }
        if should_close_position(current_return, stats.peak_return) {
            stats.closing = true;
            println!(
                "[{}] 触发移动止盈/止损，当前收益 {:+.3}% (峰值 {:+.3}%)。",
                position.asset,
                current_return * 100.0,
                stats.peak_return * 100.0
            );
            return true;
        }
        false
    }

    async fn finish_closing(&self, asset: &str, success: bool) {
        let mut tracked = self.tracked.lock().await;
        if success {
            tracked.remove(asset);
        } else if let Some(stats) = tracked.get_mut(asset) {
            stats.closing = false;
        }
    }

    async fn force_close_position(
        self: Arc<Self>,
        position: ActivePosition,
    ) -> Result<(), SharedError> {
        for attempt in 0..CLOSE_ORDER_ATTEMPTS {
            let Some(snapshot) = self.lookup_position(&position.asset).await? else {
                println!("[{}] 平仓前仓位已消失。", position.asset);
                return Ok(());
            };
            if snapshot.size <= MIN_POSITION_SIZE {
                return Ok(());
            }
            let exit_is_buy = !snapshot.is_long;
            let Some(limit_price) = self
                .trader
                .best_opposite_price(&position.asset, exit_is_buy)
                .await?
            else {
                return Err(config_error(format!(
                    "[{}] 平仓时无法获取对手价",
                    position.asset
                )));
            };
            let rounded_size = self
                .trader
                .round_order_size(&position.asset, snapshot.size)?;
            let rounded_price = self
                .trader
                .round_limit_price(&position.asset, limit_price)?;
            println!(
                "[{asset}] 平仓第 {attempt}/{total} 次尝试，限价 {price:.6}，数量 {size:.6}。",
                asset = position.asset,
                attempt = attempt + 1,
                total = CLOSE_ORDER_ATTEMPTS,
                price = rounded_price,
                size = rounded_size
            );
            let statuses = self
                .trader
                .place_limit_order(
                    &position.asset,
                    exit_is_buy,
                    rounded_size,
                    rounded_price,
                    "Gtc",
                    true,
                )
                .await?;
            log_exchange_statuses(&position.asset, &statuses);
            if statuses
                .iter()
                .any(|status| matches!(status, ExchangeDataStatus::Filled(_)))
            {
                return Ok(());
            }

            let resting = extract_resting_order_id(&statuses);
            sleep(Duration::from_secs(CLOSE_ORDER_TIMEOUT_SECS)).await;
            if let Some(oid) = resting {
                if let Err(err) = self.trader.cancel_order(&position.asset, oid).await {
                    eprintln!(
                        "[{}] 平仓限价单撤单失败 (oid={oid}): {err}",
                        position.asset
                    );
                }
            }
        }

        if let Some(snapshot) = self.lookup_position(&position.asset).await? {
            if snapshot.size > MIN_POSITION_SIZE {
                let rounded_size = self
                    .trader
                    .round_order_size(&position.asset, snapshot.size)?;
                println!(
                    "[{}] 三次限价平仓失败，将使用市价单平仓。",
                    position.asset
                );
                let statuses = self
                    .trader
                    .place_market_order(&position.asset, !snapshot.is_long, rounded_size, None)
                    .await?;
                log_exchange_statuses(&position.asset, &statuses);
            }
        }
        Ok(())
    }

    async fn lookup_position(
        &self,
        asset: &str,
    ) -> Result<Option<PositionSnapshot>, SharedError> {
        let snapshot = self.trader.user_state().await?;
        Ok(snapshot.asset_positions.into_iter().find_map(|position| {
            if position.position.coin != asset {
                return None;
            }
            let size = parse_decimal(&position.position.szi)?;
            if size.abs() <= MIN_POSITION_SIZE {
                return None;
            }
            Some(PositionSnapshot {
                is_long: size > 0.0,
                size: size.abs(),
            })
        }))
    }
}

#[derive(Clone)]
struct ActivePosition {
    asset: String,
    is_long: bool,
    entry_price: f64,
}

impl ActivePosition {
    fn return_from_price(&self, latest_price: f64) -> f64 {
        let pnl = if self.entry_price > 0.0 {
            latest_price / self.entry_price - 1.0
        } else {
            0.0
        };
        if self.is_long { pnl } else { -pnl }
    }
}

struct PositionSnapshot {
    is_long: bool,
    size: f64,
}

struct PositionStats {
    entry_price: f64,
    is_long: bool,
    peak_return: f64,
    closing: bool,
}

impl PositionStats {
    fn new(entry_price: f64, is_long: bool, current_return: f64) -> Self {
        Self {
            entry_price,
            is_long,
            peak_return: current_return.max(0.0),
            closing: false,
        }
    }

    fn reset(&mut self, entry_price: f64, is_long: bool, current_return: f64) {
        self.entry_price = entry_price;
        self.is_long = is_long;
        self.peak_return = current_return.max(0.0);
        self.closing = false;
    }

    fn needs_reset(&self, entry_price: f64, is_long: bool) -> bool {
        (self.entry_price - entry_price).abs() > PRICE_RESET_EPSILON || self.is_long != is_long
    }
}

fn parse_active_position(position: AssetPosition) -> Option<ActivePosition> {
    let entry_px = position.position.entry_px?;
    let entry_price = parse_decimal(&entry_px)?;
    if entry_price <= 0.0 {
        return None;
    }
    let size = parse_decimal(&position.position.szi)?;
    if size.abs() <= MIN_POSITION_SIZE {
        return None;
    }
    Some(ActivePosition {
        asset: position.position.coin,
        is_long: size > 0.0,
        entry_price,
    })
}

fn should_close_position(current_return: f64, peak_return: f64) -> bool {
    if current_return >= TAKE_PROFIT_RETURN {
        return true;
    }
    if current_return <= -STOP_LOSS_RETURN {
        return true;
    }
    if peak_return >= TRAILING_STOP_THRESHOLD {
        let drawdown = peak_return - current_return;
        if drawdown >= TRAILING_STOP_THRESHOLD {
            return true;
        }
        if peak_return > 0.0 && drawdown / peak_return >= TRAILING_DRAWDOWN_RATIO {
            return true;
        }
    }
    false
}

fn parse_decimal(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

pub fn spawn_signal_executor(
    trader: Arc<HyperliquidTrader>,
    risk: Arc<RiskController>,
    receiver: SignalReceiver,
) -> JoinHandle<()> {
    tokio::spawn(signal_execution_loop(trader, risk, receiver))
}

async fn signal_execution_loop(
    trader: Arc<HyperliquidTrader>,
    risk: Arc<RiskController>,
    mut receiver: SignalReceiver,
) {
    while let Some(signal) = receiver.recv().await {
        if let Err(err) = handle_trade_signal(trader.clone(), risk.clone(), signal).await {
            eprintln!("处理交易信号失败: {err}");
        }
    }
}

async fn handle_trade_signal(
    trader: Arc<HyperliquidTrader>,
    risk: Arc<RiskController>,
    signal: TradeSignal,
) -> Result<(), SharedError> {
    if let Err(reason) = risk.check_entry_permitted().await {
        println!(
            "[{}] 风控限制，跳过信号: {}",
            signal.asset(),
            reason.message()
        );
        return Ok(());
    }
    let asset = signal.asset().to_string();
    let direction_text = if signal.is_buy { "多单" } else { "空单" };
    let amplitude_percent = signal.amplitude * 100.0;

    let Some(bbo_price) = trader
        .best_opposite_price(&asset, signal.is_buy)
        .await?
    else {
        eprintln!(
            "[{asset}] 无法获取 Hyperliquid BBO 价格，跳过下单 (Binance={binance}).",
            binance = signal.binance_symbol()
        );
        return Ok(());
    };

    let order_size = ORDER_NOTIONAL_USD / bbo_price;

    let rounded_size = trader.round_order_size(&asset, order_size)?;
    let rounded_price = trader.round_limit_price(&asset, bbo_price)?;

    println!(
        "[{asset}] Binance 振幅 {amp:+.3}% 触发 {direction_text} 信号，在 Hyperliquid 以 BBO {bbo:.6} (四舍后 {rounded_px:.6}) 挂出名义 {notional} USDC (~{size:.6} 张，精度后 {rounded:.6}) 限价单。",
        amp = amplitude_percent,
        bbo = bbo_price,
        rounded_px = rounded_price,
        notional = ORDER_NOTIONAL_USD,
        size = order_size,
        rounded = rounded_size,
    );

    let statuses = trader
        .place_limit_order(
            &asset,
            signal.is_buy,
            rounded_size,
            rounded_price,
            "Gtc",
            false,
        )
        .await?;
    log_exchange_statuses(&asset, &statuses);

    if statuses
        .iter()
        .any(|status| matches!(status, ExchangeDataStatus::Filled(_)))
    {
        println!("[{asset}] 信号订单立即成交。");
        return Ok(());
    }

    if let Some(oid) = extract_resting_order_id(&statuses) {
        let cancel_trader = trader.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(ORDER_CANCEL_DELAY_MS)).await;
            if let Err(err) = cancel_trader.cancel_order(&asset, oid).await {
                eprintln!("[{asset}] 撤单失败 (oid={oid}): {err}");
            } else {
                println!("[{asset}] 2 秒未成交，已尝试撤单 (oid={oid}).");
            }
        });
    }

    Ok(())
}

fn extract_resting_order_id(statuses: &[ExchangeDataStatus]) -> Option<u64> {
    statuses.iter().find_map(|status| match status {
        ExchangeDataStatus::Resting(order) => Some(order.oid),
        _ => None,
    })
}

fn round_to_decimals(value: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn round_to_significant_and_decimal(value: f64, sig_figs: u32, max_decimals: u32) -> f64 {
    let abs_value = value.abs();
    if abs_value == 0.0 {
        return 0.0;
    }
    let magnitude = abs_value.log10().floor() as i32;
    let scale = 10f64.powi(sig_figs as i32 - magnitude - 1);
    let rounded = (abs_value * scale).round() / scale;
    round_to_decimals(rounded.copysign(value), max_decimals)
}

fn log_exchange_statuses(asset: &str, statuses: &[ExchangeDataStatus]) {
    if statuses.is_empty() {
        println!("[{asset}] 交易接口未返回任何状态。");
        return;
    }

    for status in statuses {
        match status {
            ExchangeDataStatus::Filled(order) => println!(
                "[{asset}] 订单成交，oid={}，均价={}，数量={}",
                order.oid, order.avg_px, order.total_sz
            ),
            ExchangeDataStatus::Resting(order) => {
                println!("[{asset}] 订单挂单中，oid={}", order.oid);
            }
            ExchangeDataStatus::Error(err) => {
                eprintln!("[{asset}] Hyperliquid 返回错误: {err}");
            }
            ExchangeDataStatus::WaitingForFill => {
                println!("[{asset}] 订单等待成交反馈。");
            }
            ExchangeDataStatus::WaitingForTrigger => {
                println!("[{asset}] 条件单等待触发。");
            }
            ExchangeDataStatus::Success => {
                println!("[{asset}] 订单提交成功。");
            }
        }
    }
}

fn unwrap_exchange_statuses(
    response: ExchangeResponseStatus,
) -> Result<Vec<ExchangeDataStatus>, SharedError> {
    match response {
        ExchangeResponseStatus::Ok(ok) => Ok(ok.data.map(|data| data.statuses).unwrap_or_else(Vec::new)),
        ExchangeResponseStatus::Err(err) => Err(config_error(format!("exchange error: {err}"))),
    }
}

#[derive(Clone, Debug)]
struct BboQuote {
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    _last_update_ms: u64,
    _last_refresh: Instant,
}

impl BboQuote {
    fn best_price(&self, is_buy: bool) -> Option<f64> {
        if is_buy {
            self.best_ask
        } else {
            self.best_bid
        }
    }
}
