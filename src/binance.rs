use crate::{
    trading::{
        spawn_signal_executor, HyperliquidTrader, PositionMonitor, RiskController, SignalSender,
        TradeSignal,
    },
    SharedError,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const BINANCE_PERCENT_THRESHOLD: f64 = 0.0035;
const BINANCE_WINDOW_MS: u64 = 500;
const BINANCE_SIGNAL_COOLDOWN_MS: u64 = 500;
const BINANCE_RECONNECT_SECS: u64 = 3;
const BINANCE_WS_ENDPOINT: &str = "wss://stream.binance.com:9443/ws";
const MAX_STREAMS_PER_REQUEST: usize = 100;
const SIGNAL_TRADE_COOLDOWN_SECS: u64 = 60;
const BLOCKED_SYMBOLS_PATH: &str = "blocked_symbols.json";

pub async fn stream_binance_signals(
    simbols: &[String],
    trader: Arc<HyperliquidTrader>,
) -> Result<(), SharedError> {
    let pairs = BinanceSymbolMap::new(simbols).into_pairs();
    if pairs.is_empty() {
        println!("没有可映射到 Binance 的币种，监听结束。");
        return Ok(());
    }

    println!(
        "已匹配 {} 个 Binance 符号，开始通过 SUBSCRIBE 接口监听。",
        pairs.len()
    );

    let blocked_symbols = Arc::new(load_blocked_symbols()?);
    let risk = Arc::new(RiskController::new());
    let _position_monitor = PositionMonitor::spawn(trader.clone(), risk.clone());
    let mut states: HashMap<String, SymbolState> = pairs
        .iter()
        .map(|pair| {
            (
                pair.binance_symbol.clone(),
                SymbolState::new(pair.clone(), blocked_symbols.clone()),
            )
        })
        .collect();
    let (signal_tx, signal_rx) = unbounded_channel();
    spawn_signal_executor(trader.clone(), risk, signal_rx);

    loop {
        println!("尝试连接 Binance WebSocket...");
        match connect_async(BINANCE_WS_ENDPOINT).await {
            Ok((mut socket, _)) => {
                println!(
                    "Binance WebSocket 已连接，准备订阅 {} 个币种。",
                    pairs.len()
                );
                if let Err(err) = subscribe_pairs(&mut socket, &pairs).await {
                    eprintln!("发送订阅请求失败: {err}");
                    continue;
                }

                while let Some(msg) = socket.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            if let Err(err) =
                                handle_binance_text(&text, &mut states, &signal_tx)
                            {
                                eprintln!("处理 Binance 消息失败: {err}");
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Ping(payload)) => {
                            if let Err(err) = socket
                                .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                                .await
                            {
                                eprintln!("发送 Pong 失败: {err}");
                                break;
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                            println!("Binance WebSocket 收到关闭帧: {:?}", frame);
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("Binance WebSocket 错误: {err}");
                            break;
                        }
                    }
                }
            }
            Err(err) => eprintln!("连接 Binance WebSocket 失败: {err}"),
        }

        println!(
            "Binance WebSocket 已断开，{} 秒后尝试重连...",
            BINANCE_RECONNECT_SECS
        );
        sleep(Duration::from_secs(BINANCE_RECONNECT_SECS)).await;
    }
}

async fn subscribe_pairs(
    socket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    pairs: &[BinancePair],
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let mut request_id: u64 = 1;
    for chunk in pairs.chunks(MAX_STREAMS_PER_REQUEST) {
        let params: Vec<String> = chunk
            .iter()
            .map(|pair| format!("{}@bookTicker", pair.binance_symbol.to_lowercase()))
            .collect();
        let req = json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": request_id,
        });
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                req.to_string(),
            ))
            .await?;
        request_id += 1;
    }
    Ok(())
}

fn handle_binance_text(
    text: &str,
    states: &mut HashMap<String, SymbolState>,
    signal_sender: &SignalSender,
) -> Result<(), serde_json::Error> {
    match serde_json::from_str::<BinanceMessage>(text)? {
        BinanceMessage::Response(resp) => {
            if let Some(code) = resp.code {
                eprintln!("Binance 订阅错误 ({code}): {:?}", resp.msg);
            } else {
                println!(
                    "Binance 订阅响应: id={:?}, result={:?}",
                    resp.id, resp.result
                );
            }
        }
        BinanceMessage::Event(ticker) => {
            dispatch_signal(process_ticker(ticker, states), signal_sender)
        }
        BinanceMessage::Combined { data, .. } => {
            dispatch_signal(process_ticker(data, states), signal_sender)
        }
    }
    Ok(())
}

fn process_ticker(
    ticker: BinanceBookTicker,
    states: &mut HashMap<String, SymbolState>,
) -> Option<TradeSignal> {
    let symbol = ticker.symbol.to_uppercase();
    if let Some(state) = states.get_mut(&symbol) {
        if let (Ok(bid), Ok(ask)) = (
            ticker.best_bid.parse::<f64>(),
            ticker.best_ask.parse::<f64>(),
        ) {
            let price = (bid + ask) / 2.0;
            return state.on_price(price);
        }
    }
    None
}

fn dispatch_signal(event: Option<TradeSignal>, sender: &SignalSender) {
    if let Some(signal) = event {
        if sender.send(signal).is_err() {
            eprintln!("交易信号通道已关闭，无法下单。");
        }
    }
}

fn load_blocked_symbols() -> Result<HashSet<String>, SharedError> {
    match fs::read_to_string(BLOCKED_SYMBOLS_PATH) {
        Ok(contents) => {
            let list: Vec<String> = serde_json::from_str(&contents)?;
            let sanitized = list
                .into_iter()
                .map(|s| s.trim().to_ascii_uppercase())
                .filter(|s| !s.is_empty())
                .collect::<HashSet<String>>();
            println!(
                "已从 {BLOCKED_SYMBOLS_PATH} 读取 {} 个屏蔽币种。",
                sanitized.len()
            );
            Ok(sanitized)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            println!(
                "未找到 {BLOCKED_SYMBOLS_PATH}，将默认允许所有币种。可创建该文件并写入 JSON 数组屏蔽指定币种。"
            );
            Ok(HashSet::new())
        }
        Err(err) => Err(Box::new(err)),
    }
}

#[derive(Clone, Debug)]
struct BinancePair {
    binance_symbol: String,
    display_symbol: String,
}

struct BinanceSymbolMap {
    pairs: Vec<BinancePair>,
}

impl BinanceSymbolMap {
    fn new(symbols: &[String]) -> Self {
        let mut pairs = Vec::new();
        for symbol in symbols {
            if let Some(mapped) = map_symbol_to_binance(symbol) {
                pairs.push(BinancePair {
                    binance_symbol: mapped,
                    display_symbol: symbol.clone(),
                });
            } else {
                eprintln!("无法将 {symbol} 映射到 Binance，已跳过。");
            }
        }
        Self { pairs }
    }

    fn into_pairs(self) -> Vec<BinancePair> {
        self.pairs
    }
}

fn map_symbol_to_binance(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut candidate = trimmed.to_string();
    if candidate.len() > 1 {
        let mut chars = candidate.chars();
        if matches!(chars.next(), Some('k' | 'K')) {
            candidate = chars.collect();
        }
    }

    let cleaned: String = candidate
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    if cleaned.is_empty() {
        return None;
    }

    let upper = cleaned.to_uppercase();
    if upper.ends_with("USDT") {
        Some(upper)
    } else {
        Some(format!("{upper}USDT"))
    }
}

struct SymbolState {
    pair: BinancePair,
    window: PriceWindow,
    last_detection: Option<Instant>,
    last_trade_signal: Option<Instant>,
    blocked_symbols: Arc<HashSet<String>>,
}

impl SymbolState {
    fn new(pair: BinancePair, blocked_symbols: Arc<HashSet<String>>) -> Self {
        Self {
            pair,
            window: PriceWindow::new(),
            last_detection: None,
            last_trade_signal: None,
            blocked_symbols,
        }
    }

    fn on_price(&mut self, price: f64) -> Option<TradeSignal> {
        self.window.push_price(price);

        if let Some(change) = self.window.rate_of_change() {
            if change.abs() >= BINANCE_PERCENT_THRESHOLD && self.can_emit_signal() {
                if !self.can_emit_trade_signal() {
                    return None;
                }
                if self.is_blocked() {
                    println!(
                        "[{}] 已在屏蔽列表中，本次振幅信号被忽略。",
                        self.pair.display_symbol
                    );
                    return None;
                }
                let direction = if change > 0.0 { "多单" } else { "空单" };
                let amplitude_percent = change * 100.0;
                println!(
                    "[{symbol}] {direction}信号 | 振幅 {amp:+.3}% | 最新价 {price:.6}",
                    symbol = self.pair.display_symbol,
                    direction = direction,
                    amp = amplitude_percent,
                    price = price
                );
                return Some(TradeSignal {
                    asset: self.pair.display_symbol.clone(),
                    binance_symbol: self.pair.binance_symbol.clone(),
                    is_buy: change > 0.0,
                    amplitude: change,
                });
            }
        }
        None
    }

    fn can_emit_signal(&mut self) -> bool {
        let now = Instant::now();
        let cooldown = Duration::from_millis(BINANCE_SIGNAL_COOLDOWN_MS);
        if let Some(last) = self.last_detection {
            if now.duration_since(last) < cooldown {
                return false;
            }
        }
        self.last_detection = Some(now);
        true
    }

    fn can_emit_trade_signal(&mut self) -> bool {
        let now = Instant::now();
        let cooldown = Duration::from_secs(SIGNAL_TRADE_COOLDOWN_SECS);
        if let Some(last) = self.last_trade_signal {
            if now.duration_since(last) < cooldown {
                return false;
            }
        }
        self.last_trade_signal = Some(now);
        true
    }

    fn is_blocked(&self) -> bool {
        let symbol = self.pair.display_symbol.to_ascii_uppercase();
        self.blocked_symbols.contains(&symbol)
    }
}

struct PriceWindow {
    entries: VecDeque<(Instant, f64)>,
}

impl PriceWindow {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    fn push_price(&mut self, price: f64) {
        let now = Instant::now();
        self.entries.push_back((now, price));
        let window = Duration::from_millis(BINANCE_WINDOW_MS);
        while let Some((ts, _)) = self.entries.front() {
            if now.duration_since(*ts) > window {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    fn rate_of_change(&self) -> Option<f64> {
        let (_, first_price) = self.entries.front()?;
        let (_, last_price) = self.entries.back()?;
        if *first_price <= 0.0 {
            return None;
        }
        Some((last_price - first_price) / first_price)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BinanceMessage {
    Event(BinanceBookTicker),
    Combined {
        #[allow(dead_code)]
        stream: String,
        data: BinanceBookTicker,
    },
    Response(BinanceResponse),
}

#[derive(Deserialize)]
struct BinanceResponse {
    result: Option<serde_json::Value>,
    id: Option<u64>,
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Deserialize, Clone)]
struct BinanceBookTicker {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid: String,
    #[serde(rename = "a")]
    best_ask: String,
}
