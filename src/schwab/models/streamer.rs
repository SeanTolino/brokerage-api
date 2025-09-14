// src/schwab/models/streamer.rs

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone)]
pub enum StreamerMessage {
    LevelOneEquity(LevelOneEquitiesResponse),
    LevelOneOption(LevelOneOptionsResponse),
    // We can add more variants here for other data types in the future
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum LevelOneOptionsField {
    Symbol,
    Description,
    BidPrice,
    AskPrice,
    LastPrice,
    HighPrice,
    LowPrice,
    ClosePrice,
    TotalVolume,
    OpenInterest,
    Volatility,
    MoneyIntrinsicValue,
    ExpirationYear,
    Multiplier,
    Digits,
    OpenPrice,
    BidSize,
    AskSize,
    LastSize,
    NetChange,
    StrikePrice,
    ContractType,
    Underlying,
    ExpirationMonth,
    Deliverables,
    TimeValue,
    ExpirationDay,
    DaysToExpiration,
    Delta,
    Gamma,
    Theta,
    Vega,
    Rho,
    SecurityStatus,
    TheoreticalOptionValue,
    UnderlyingPrice,
    UvExpirationType,
    MarkPrice,
    QuoteTimeInLong,
    TradeTimeInLong,
    Exchange,
    ExchangeName,
    LastTradingDay,
    SettlementType,
    NetPercentChange,
    MarkPriceNetChange,
    MarkPricePercentChange,
    ImpliedYield,
    IsPennyPilot,
    OptionRoot,
    FiftyTwoWeekHigh,
    FiftyTwoWeekLow,
    IndicativeAskPrice,
    IndicativeBidPrice,
    IndicativeQuoteTime,
    ExerciseType,
}

impl fmt::Display for LevelOneOptionsField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LevelOneOptionsField::Symbol => write!(f, "0"),
            LevelOneOptionsField::Description => write!(f, "1"),
            LevelOneOptionsField::BidPrice => write!(f, "2"),
            LevelOneOptionsField::AskPrice => write!(f, "3"),
            LevelOneOptionsField::LastPrice => write!(f, "4"),
            LevelOneOptionsField::HighPrice => write!(f, "5"),
            LevelOneOptionsField::LowPrice => write!(f, "6"),
            LevelOneOptionsField::ClosePrice => write!(f, "7"),
            LevelOneOptionsField::TotalVolume => write!(f, "8"),
            LevelOneOptionsField::OpenInterest => write!(f, "9"),
            LevelOneOptionsField::Volatility => write!(f, "10"),
            LevelOneOptionsField::MoneyIntrinsicValue => write!(f, "11"),
            LevelOneOptionsField::ExpirationYear => write!(f, "12"),
            LevelOneOptionsField::Multiplier => write!(f, "13"),
            LevelOneOptionsField::Digits => write!(f, "14"),
            LevelOneOptionsField::OpenPrice => write!(f, "15"),
            LevelOneOptionsField::BidSize => write!(f, "16"),
            LevelOneOptionsField::AskSize => write!(f, "17"),
            LevelOneOptionsField::LastSize => write!(f, "18"),
            LevelOneOptionsField::NetChange => write!(f, "19"),
            LevelOneOptionsField::StrikePrice => write!(f, "20"),
            LevelOneOptionsField::ContractType => write!(f, "21"),
            LevelOneOptionsField::Underlying => write!(f, "22"),
            LevelOneOptionsField::ExpirationMonth => write!(f, "23"),
            LevelOneOptionsField::Deliverables => write!(f, "24"),
            LevelOneOptionsField::TimeValue => write!(f, "25"),
            LevelOneOptionsField::ExpirationDay => write!(f, "26"),
            LevelOneOptionsField::DaysToExpiration => write!(f, "27"),
            LevelOneOptionsField::Delta => write!(f, "28"),
            LevelOneOptionsField::Gamma => write!(f, "29"),
            LevelOneOptionsField::Theta => write!(f, "30"),
            LevelOneOptionsField::Vega => write!(f, "31"),
            LevelOneOptionsField::Rho => write!(f, "32"),
            LevelOneOptionsField::SecurityStatus => write!(f, "33"),
            LevelOneOptionsField::TheoreticalOptionValue => write!(f, "34"),
            LevelOneOptionsField::UnderlyingPrice => write!(f, "35"),
            LevelOneOptionsField::UvExpirationType => write!(f, "36"),
            LevelOneOptionsField::MarkPrice => write!(f, "37"),
            LevelOneOptionsField::QuoteTimeInLong => write!(f, "38"),
            LevelOneOptionsField::TradeTimeInLong => write!(f, "39"),
            LevelOneOptionsField::Exchange => write!(f, "40"),
            LevelOneOptionsField::ExchangeName => write!(f, "41"),
            LevelOneOptionsField::LastTradingDay => write!(f, "42"),
            LevelOneOptionsField::SettlementType => write!(f, "43"),
            LevelOneOptionsField::NetPercentChange => write!(f, "44"),
            LevelOneOptionsField::MarkPriceNetChange => write!(f, "45"),
            LevelOneOptionsField::MarkPricePercentChange => write!(f, "46"),
            LevelOneOptionsField::ImpliedYield => write!(f, "47"),
            LevelOneOptionsField::IsPennyPilot => write!(f, "48"),
            LevelOneOptionsField::OptionRoot => write!(f, "49"),
            LevelOneOptionsField::FiftyTwoWeekHigh => write!(f, "50"),
            LevelOneOptionsField::FiftyTwoWeekLow => write!(f, "51"),
            LevelOneOptionsField::IndicativeAskPrice => write!(f, "52"),
            LevelOneOptionsField::IndicativeBidPrice => write!(f, "53"),
            LevelOneOptionsField::IndicativeQuoteTime => write!(f, "54"),
            LevelOneOptionsField::ExerciseType => write!(f, "55"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum LevelOneEquitiesField {
    Symbol,
    BidPrice,
    AskPrice,
    LastPrice,
    BidSize,
    AskSize,
    AskId,
    BidId,
    TotalVolume,
    LastSize,
    HighPrice,
    LowPrice,
    ClosePrice,
    ExchangeId,
    Marginable,
    Description,
    LastId,
    OpenPrice,
    NetChange,
    FiftyTwoWeekHigh,
    FiftyTwoWeekLow,
    PeRatio,
    AnnualDividendAmount,
    DividendYield,
    Nav,
    ExchangeName,
    DueDate,
    RegularMarketQuote,
    RegularMarketTrade,
    RegularMarketLastPrice,
    RegularMarketLastSize,
    RegularMarketNetChange,
    SecurityStatus,
    MarkPrice,
    QuoteTimeInLong,
    TradeTimeInLong,
    RegularMarketTradeTimeInLong,
    BidTime,
    AskTime,
    AskMicId,
    BidMicId,
    LastMicId,
    NetPercentChange,
    RegularMarketPercentChange,
    MarkPriceNetChange,
    MarkPricePercentChange,
    HardToBorrowQuantity,
    HardToBorrowRate,
    HardToBorrow,
    Shortable,
    PostMarketNetChange,
    PostMarketPercentChange,
}

impl fmt::Display for LevelOneEquitiesField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LevelOneEquitiesField::Symbol => write!(f, "0"),
            LevelOneEquitiesField::BidPrice => write!(f, "1"),
            LevelOneEquitiesField::AskPrice => write!(f, "2"),
            LevelOneEquitiesField::LastPrice => write!(f, "3"),
            LevelOneEquitiesField::BidSize => write!(f, "4"),
            LevelOneEquitiesField::AskSize => write!(f, "5"),
            LevelOneEquitiesField::AskId => write!(f, "6"),
            LevelOneEquitiesField::BidId => write!(f, "7"),
            LevelOneEquitiesField::TotalVolume => write!(f, "8"),
            LevelOneEquitiesField::LastSize => write!(f, "9"),
            LevelOneEquitiesField::HighPrice => write!(f, "10"),
            LevelOneEquitiesField::LowPrice => write!(f, "11"),
            LevelOneEquitiesField::ClosePrice => write!(f, "12"),
            LevelOneEquitiesField::ExchangeId => write!(f, "13"),
            LevelOneEquitiesField::Marginable => write!(f, "14"),
            LevelOneEquitiesField::Description => write!(f, "15"),
            LevelOneEquitiesField::LastId => write!(f, "16"),
            LevelOneEquitiesField::OpenPrice => write!(f, "17"),
            LevelOneEquitiesField::NetChange => write!(f, "18"),
            LevelOneEquitiesField::FiftyTwoWeekHigh => write!(f, "19"),
            LevelOneEquitiesField::FiftyTwoWeekLow => write!(f, "20"),
            LevelOneEquitiesField::PeRatio => write!(f, "21"),
            LevelOneEquitiesField::AnnualDividendAmount => write!(f, "22"),
            LevelOneEquitiesField::DividendYield => write!(f, "23"),
            LevelOneEquitiesField::Nav => write!(f, "24"),
            LevelOneEquitiesField::ExchangeName => write!(f, "25"),
            LevelOneEquitiesField::DueDate => write!(f, "26"),
            LevelOneEquitiesField::RegularMarketQuote => write!(f, "27"),
            LevelOneEquitiesField::RegularMarketTrade => write!(f, "28"),
            LevelOneEquitiesField::RegularMarketLastPrice => write!(f, "29"),
            LevelOneEquitiesField::RegularMarketLastSize => write!(f, "30"),
            LevelOneEquitiesField::RegularMarketNetChange => write!(f, "31"),
            LevelOneEquitiesField::SecurityStatus => write!(f, "32"),
            LevelOneEquitiesField::MarkPrice => write!(f, "33"),
            LevelOneEquitiesField::QuoteTimeInLong => write!(f, "34"),
            LevelOneEquitiesField::TradeTimeInLong => write!(f, "35"),
            LevelOneEquitiesField::RegularMarketTradeTimeInLong => write!(f, "36"),
            LevelOneEquitiesField::BidTime => write!(f, "37"),
            LevelOneEquitiesField::AskTime => write!(f, "38"),
            LevelOneEquitiesField::AskMicId => write!(f, "39"),
            LevelOneEquitiesField::BidMicId => write!(f, "40"),
            LevelOneEquitiesField::LastMicId => write!(f, "41"),
            LevelOneEquitiesField::NetPercentChange => write!(f, "42"),
            LevelOneEquitiesField::RegularMarketPercentChange => write!(f, "43"),
            LevelOneEquitiesField::MarkPriceNetChange => write!(f, "44"),
            LevelOneEquitiesField::MarkPricePercentChange => write!(f, "45"),
            LevelOneEquitiesField::HardToBorrowQuantity => write!(f, "46"),
            LevelOneEquitiesField::HardToBorrowRate => write!(f, "47"),
            LevelOneEquitiesField::HardToBorrow => write!(f, "48"),
            LevelOneEquitiesField::Shortable => write!(f, "49"),
            LevelOneEquitiesField::PostMarketNetChange => write!(f, "50"),
            LevelOneEquitiesField::PostMarketPercentChange => write!(f, "51"),
        }
    }
}

// --- FIXED: Added #[serde(rename = ...)] attributes to all fields ---
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LevelOneOptionsResponse {
    #[serde(rename = "key")]
    pub symbol: String,
    #[serde(rename = "1")]
    pub description: Option<String>,
    #[serde(rename = "2")]
    pub bid_price: Option<f64>,
    #[serde(rename = "3")]
    pub ask_price: Option<f64>,
    #[serde(rename = "4")]
    pub last_price: Option<f64>,
    #[serde(rename = "5")]
    pub high_price: Option<f64>,
    #[serde(rename = "6")]
    pub low_price: Option<f64>,
    #[serde(rename = "7")]
    pub close_price: Option<f64>,
    #[serde(rename = "8")]
    pub total_volume: Option<i64>,
    #[serde(rename = "9")]
    pub open_interest: Option<i64>,
    #[serde(rename = "10")]
    pub volatility: Option<f64>,
    #[serde(rename = "11")]
    pub money_intrinsic_value: Option<f64>,
    #[serde(rename = "12")]
    pub expiration_year: Option<i64>,
    #[serde(rename = "13")]
    pub multiplier: Option<f64>,
    #[serde(rename = "14")]
    pub digits: Option<i64>,
    #[serde(rename = "15")]
    pub open_price: Option<f64>,
    #[serde(rename = "16")]
    pub bid_size: Option<i64>,
    #[serde(rename = "17")]
    pub ask_size: Option<i64>,
    #[serde(rename = "18")]
    pub last_size: Option<i64>,
    #[serde(rename = "19")]
    pub net_change: Option<f64>,
    #[serde(rename = "20")]
    pub strike_price: Option<f64>,
    #[serde(rename = "21")]
    pub contract_type: Option<String>, // "CALL" or "PUT"
    #[serde(rename = "22")]
    pub underlying: Option<String>,
    #[serde(rename = "23")]
    pub expiration_month: Option<i64>,
    #[serde(rename = "24")]
    pub deliverables: Option<String>,
    #[serde(rename = "25")]
    pub time_value: Option<f64>,
    #[serde(rename = "26")]
    pub expiration_day: Option<i64>,
    #[serde(rename = "27")]
    pub days_to_expiration: Option<i64>,
    #[serde(rename = "28")]
    pub delta: Option<f64>,
    #[serde(rename = "29")]
    pub gamma: Option<f64>,
    #[serde(rename = "30")]
    pub theta: Option<f64>,
    #[serde(rename = "31")]
    pub vega: Option<f64>,
    #[serde(rename = "32")]
    pub rho: Option<f64>,
    #[serde(rename = "33")]
    pub security_status: Option<String>,
    #[serde(rename = "34")]
    pub theoretical_option_value: Option<f64>,
    #[serde(rename = "35")]
    pub underlying_price: Option<f64>,
    #[serde(rename = "36")]
    pub uv_expiration_type: Option<String>,
    #[serde(rename = "37")]
    pub mark_price: Option<f64>,
    #[serde(rename = "38")]
    pub quote_time_in_long: Option<i64>,
    #[serde(rename = "39")]
    pub trade_time_in_long: Option<i64>,
    #[serde(rename = "40")]
    pub exchange: Option<String>,
    #[serde(rename = "41")]
    pub exchange_name: Option<String>,
    #[serde(rename = "42")]
    pub last_trading_day: Option<i64>,
    #[serde(rename = "43")]
    pub settlement_type: Option<String>,
    #[serde(rename = "44")]
    pub net_percent_change: Option<f64>,
    #[serde(rename = "45")]
    pub mark_price_net_change: Option<f64>,
    #[serde(rename = "46")]
    pub mark_price_percent_change: Option<f64>,
    #[serde(rename = "47")]
    pub implied_yield: Option<f64>,
    #[serde(rename = "48")]
    pub is_penny_pilot: Option<bool>,
    #[serde(rename = "49")]
    pub option_root: Option<String>,
    #[serde(rename = "50")]
    pub fifty_two_week_high: Option<f64>,
    #[serde(rename = "51")]
    pub fifty_two_week_low: Option<f64>,
    #[serde(rename = "52")]
    pub indicative_ask_price: Option<f64>,
    #[serde(rename = "53")]
    pub indicative_bid_price: Option<f64>,
    #[serde(rename = "54")]
    pub indicative_quote_time: Option<i64>,
    #[serde(rename = "55")]
    pub exercise_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LevelOneEquitiesResponse {
    #[serde(rename = "key")]
    pub symbol: String,
    #[serde(rename = "1")]
    pub bid_price: Option<f64>,
    #[serde(rename = "2")]
    pub ask_price: Option<f64>,
    #[serde(rename = "3")]
    pub last_price: Option<f64>,
    #[serde(rename = "4")]
    pub bid_size: Option<i64>,
    #[serde(rename = "5")]
    pub ask_size: Option<i64>,
    #[serde(rename = "6")]
    pub ask_id: Option<char>,
    #[serde(rename = "7")]
    pub bid_id: Option<char>,
    #[serde(rename = "8")]
    pub total_volume: Option<i64>,
    #[serde(rename = "9")]
    pub last_size: Option<i64>,
    #[serde(rename = "10")]
    pub high_price: Option<f64>,
    #[serde(rename = "11")]
    pub low_price: Option<f64>,
    #[serde(rename = "12")]
    pub close_price: Option<f64>,
    #[serde(rename = "13")]
    pub exchange_id: Option<String>,
    #[serde(rename = "14")]
    pub marginable: Option<bool>,
    #[serde(rename = "15")]
    pub description: Option<String>,
    #[serde(rename = "16")]
    pub last_id: Option<char>,
    #[serde(rename = "17")]
    pub open_price: Option<f64>,
    #[serde(rename = "18")]
    pub net_change: Option<f64>,
    #[serde(rename = "19")]
    pub fifty_two_week_high: Option<f64>,
    #[serde(rename = "20")]
    pub fifty_two_week_low: Option<f64>,
    #[serde(rename = "21")]
    pub pe_ratio: Option<f64>,
    #[serde(rename = "22")]
    pub annual_dividend_amount: Option<f64>,
    #[serde(rename = "23")]
    pub dividend_yield: Option<f64>,
    #[serde(rename = "24")]
    pub nav: Option<f64>,
    #[serde(rename = "25")]
    pub exchange_name: Option<String>,
    #[serde(rename = "26")]
    pub due_date: Option<String>,
    #[serde(rename = "27")]
    pub regular_market_quote: Option<bool>,
    #[serde(rename = "28")]
    pub regular_market_trade: Option<bool>,
    #[serde(rename = "29")]
    pub regular_market_last_price: Option<f64>,
    #[serde(rename = "30")]
    pub regular_market_last_size: Option<i64>,
    #[serde(rename = "31")]
    pub regular_market_net_change: Option<f64>,
    #[serde(rename = "32")]
    pub security_status: Option<String>,
    #[serde(rename = "33")]
    pub mark_price: Option<f64>,
    #[serde(rename = "34")]
    pub quote_time_in_long: Option<i64>,
    #[serde(rename = "35")]
    pub trade_time_in_long: Option<i64>,
    #[serde(rename = "36")]
    pub regular_market_trade_time_in_long: Option<i64>,
    #[serde(rename = "37")]
    pub bid_time: Option<i64>,
    #[serde(rename = "38")]
    pub ask_time: Option<i64>,
    #[serde(rename = "39")]
    pub ask_mic_id: Option<String>,
    #[serde(rename = "40")]
    pub bid_mic_id: Option<String>,
    #[serde(rename = "41")]
    pub last_mic_id: Option<String>,
    #[serde(rename = "42")]
    pub net_percent_change: Option<f64>,
    #[serde(rename = "43")]
    pub regular_market_percent_change: Option<f64>,
    #[serde(rename = "44")]
    pub mark_price_net_change: Option<f64>,
    #[serde(rename = "45")]
    pub mark_price_percent_change: Option<f64>,
    #[serde(rename = "46")]
    pub hard_to_borrow_quantity: Option<i64>,
    #[serde(rename = "47")]
    pub hard_to_borrow_rate: Option<f64>,
    #[serde(rename = "48")]
    pub hard_to_borrow: Option<i64>,
    #[serde(rename = "49")]
    pub shortable: Option<i64>,
    #[serde(rename = "50")]
    pub post_market_net_change: Option<f64>,
    #[serde(rename = "51")]
    pub post_market_percent_change: Option<f64>,
    #[serde(rename = "assetMainType")]
    pub asset_main_type: Option<String>,
    #[serde(rename = "assetSubType")]
    pub asset_sub_type: Option<String>,
    pub cusip: Option<String>,
    pub delayed: Option<bool>,
}