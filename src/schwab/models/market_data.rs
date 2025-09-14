use serde::Deserialize;
use std::collections::HashMap;

/// The top-level response for a quotes request is a map from symbol to quote data.
pub type QuotesResponse = HashMap<String, Quote>;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quote {
    pub asset_type: String,
    pub asset_main_type: String,
    pub cusip: Option<String>,
    pub symbol: String,
    pub description: String,
    pub quote: Option<EquityQuote>,
    pub fundamental: Option<FundamentalData>,
    pub extended: Option<ExtendedQuote>,
    pub reference: Option<ReferenceData>,
    pub regular: Option<RegularMarketData>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EquityQuote {
    #[serde(rename = "52WeekHigh")]
    pub fifty_two_week_high: f64,
    #[serde(rename = "52WeekLow")]
    pub fifty_two_week_low: f64,
    pub ask_mic_id: String,
    pub ask_price: f64,
    pub ask_size: i64,
    pub bid_mic_id: String,
    pub bid_price: f64,
    pub bid_size: i64,
    pub close_price: f64,
    pub high_price: f64,
    pub last_mic_id: String,
    pub last_price: f64,
    pub last_size: i64,
    pub low_price: f64,
    pub mark: f64,
    pub net_change: f64,
    pub net_percent_change: f64,
    pub open_price: f64,
    pub quote_time_in_long: i64,
    pub security_status: String,
    pub total_volume: i64,
    pub trade_time_in_long: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundamentalData {
    pub avg10_day_volume: i64,
    pub avg1_year_volume: i64,
    pub declaration_date: String,
    pub div_amount: f64,
    pub div_ex_date: String,
    pub div_freq: i32,
    pub div_pay_date: String,
    pub div_yield: f64,
    pub eps: f64,
    pub exchange: String,
    pub high52: f64,
    pub last_earnings_date: String,
    pub low52: f64,
    pub market_cap: f64,
    pub market_cap_float: f64,
    pub pe_ratio: f64,
    pub peg_ratio: f64,
    pub pb_ratio: f64,
    pub pr_ratio: f64,
    pub pcf_ratio: f64,
    pub gross_margin_ttm: f64,
    pub net_profit_margin_ttm: f64,
    pub operating_margin_ttm: f64,
    pub return_on_equity: f64,
    pub return_on_assets: f64,
    pub return_on_investment: f64,
    pub quick_ratio: f64,
    pub current_ratio: f64,
    pub interest_coverage: f64,
    pub total_debt_to_capital: f64,
    pub lt_debt_to_equity: f64,
    pub total_debt_to_equity: f64,
    pub revenue_per_share_ttm: f64,
    pub book_value_per_share: f64,
    pub short_int_to_float: f64,
    pub short_int_day_to_cover: f64,
    pub shares_outstanding: i64,
    pub beta: f64,
    pub volatility: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtendedQuote {
    // Define fields for the "extended" object if needed
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceData {
    // Define fields for the "reference" object if needed
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegularMarketData {
    // Define fields for the "regular" object if needed
}

/// A type alias for the complex nested map of expiration dates to strikes to contracts.
pub type ExpirationMap = HashMap<String, HashMap<String, Vec<OptionContract>>>;

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum PutCall {
    Put,
    Call,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainsResponse {
    pub symbol: String,
    pub status: String,
    pub underlying: Option<UnderlyingInfo>,
    pub strategy: String,
    pub interval: f64,
    pub is_delayed: bool,
    pub is_index: bool,
    pub interest_rate: f64,
    pub underlying_price: f64,
    pub volatility: f64,
    pub days_to_expiration: f64,
    pub number_of_contracts: i64,
    #[serde(rename = "callExpDateMap")]
    pub call_exp_date_map: ExpirationMap,
    #[serde(rename = "putExpDateMap")]
    pub put_exp_date_map: ExpirationMap,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnderlyingInfo {
    pub symbol: String,
    pub description: String,
    pub change: f64,
    pub percent_change: f64,
    pub close: f64,
    pub quote_time: i64,
    pub trade_time: i64,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub mark: f64,
    pub mark_change: f64,
    pub mark_percent_change: f64,
    pub bid_size: i64,
    pub ask_size: i64,
    pub high_price: f64,
    pub low_price: f64,
    pub open_price: f64,
    pub total_volume: i64,
    #[serde(rename = "52WeekHigh")]
    pub fifty_two_week_high: f64,
    #[serde(rename = "52WeekLow")]
    pub fifty_two_week_low: f64,
    pub delayed: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionContract {
    #[serde(rename = "putCall")]
    pub put_call: PutCall,
    pub symbol: String,
    pub description: String,
    pub exchange_name: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub mark: f64,
    pub bid_size: i64,
    pub ask_size: i64,
    pub bid_ask_size: String,
    pub last_size: i64,
    pub high_price: f64,
    pub low_price: f64,
    pub open_price: f64,
    pub close_price: f64,
    pub total_volume: i64,
    pub trade_date: Option<String>,
    pub trade_time_in_long: i64,
    pub quote_time_in_long: i64,
    pub net_change: f64,
    pub volatility: f64,
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub rho: f64,
    pub open_interest: i64,
    pub time_value: f64,
    pub theoretical_option_value: f64,
    pub theoretical_volatility: f64,
    pub strike_price: f64,
    pub expiration_date: String,
    pub days_to_expiration: i64,
    pub expiration_type: String,
    pub last_trading_day: i64,
    pub multiplier: f64,
    pub settlement_type: String,
    pub deliverable_note: String,
    pub in_the_money: bool,
    pub is_penny_pilot: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceHistoryResponse {
    pub candles: Vec<Candle>,
    pub symbol: String,
    pub empty: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    /// Epoch milliseconds
    pub datetime: i64,
}

/// The response for a movers request is a list of Mover objects.
pub type MoversResponse = Vec<Mover>;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mover {
    pub change: f64,
    pub description: String,
    pub direction: String,
    pub last: f64,
    #[serde(rename = "percentChange")]
    pub percent_change: f64,
    pub symbol: String,
    #[serde(rename = "totalVolume")]
    pub total_volume: i64,
}

/// The response for an instruments request is a list of Instrument objects.
pub type InstrumentsResponse = Vec<Instrument>;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instrument {
    pub cusip: String,
    pub symbol: String,
    pub description: String,
    pub exchange: String,
    pub asset_type: String,
}

/// The response for market hours is a map of market names to their hours.
pub type MarketHoursResponse = HashMap<String, MarketHours>;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketHours {
    pub date: String,
    pub market_type: String,
    pub exchange: Option<String>,
    pub category: Option<String>,
    pub product: String,
    pub product_name: String,
    pub is_open: bool,
    #[serde(rename = "sessionHours")]
    pub session_hours: Option<HashMap<String, Vec<MarketSession>>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSession {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpirationChainResponse {
    pub expiration_list: Vec<ExpirationDate>,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpirationDate {
    pub expiration_date: String,
    pub days_to_expiration: i32,
    pub expiration_type: String,
    pub standard: bool,
}