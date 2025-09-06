use std::{fmt, sync::Arc};

use chrono::{DateTime, Utc};
use reqwest::{
    Client,
    header::{HeaderMap, HeaderValue},
};
use serde_json::Value;
use urlencoding::encode;

use crate::{
    schwab::{
        common::{SCHWAB_MARKET_DATA_API_URL, SCHWAB_TRADER_API_URL, TOKENS_FILE},
        schwab_auth::StoredTokenInfo,
    },
    util::{dedup_ordered, parse_params, time_to_epoch_ms, time_to_yyyymmdd},
};

/// Represents the type of contract for an options chain.
pub enum ContractType {
    /// Call options.
    Call,
    /// Put options.
    Put,
    /// All options (both call and put).
    All,
}

impl fmt::Display for ContractType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContractType::All => write!(f, "ALL"),
            ContractType::Call => write!(f, "CALL"),
            ContractType::Put => write!(f, "PUT"),
        }
    }
}

/// Represents the fields to be returned in a quote.
#[derive(Eq, PartialEq, Hash, Clone)]
pub enum QuoteFields {
    /// Quote data.
    Quote,
    /// Fundamental data.
    Fundamental,
    /// Extended data.
    Extended,
    /// Reference data.
    Reference,
    /// Regular data.
    Regular,
}

impl fmt::Display for QuoteFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuoteFields::Quote => write!(f, "quote"),
            QuoteFields::Fundamental => write!(f, "fundamental"),
            QuoteFields::Extended => write!(f, "extended"),
            QuoteFields::Reference => write!(f, "reference"),
            QuoteFields::Regular => write!(f, "regular"),
        }
    }
}

/// Represents the period type for price history.
#[derive(Eq, PartialEq, Hash, Clone)]
pub enum PeriodType {
    /// Day period type.
    Day,
    /// Month period type.
    Month,
    /// Year period type.
    Year,
    /// Year to date period type.
    Ytd,
}

impl fmt::Display for PeriodType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeriodType::Day => write!(f, "day"),
            PeriodType::Month => write!(f, "month"),
            PeriodType::Year => write!(f, "year"),
            PeriodType::Ytd => write!(f, "ytd"),
        }
    }
}

/// Represents the frequency type for price history.
#[derive(Eq, PartialEq, Hash, Clone)]
pub enum FrequencyType {
    /// Minute frequency type.
    Minute,
    /// Daily frequency type.
    Daily,
    /// Weekly frequency type.
    Weekly,
    /// Monthly frequency type.
    Monthly,
}

impl fmt::Display for FrequencyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrequencyType::Minute => write!(f, "minute"),
            FrequencyType::Daily => write!(f, "daily"),
            FrequencyType::Weekly => write!(f, "weekly"),
            FrequencyType::Monthly => write!(f, "monthly"),
        }
    }
}

/// Represents the sort order for movers.
#[derive(Eq, PartialEq, Hash, Clone)]
pub enum Sort {
    /// Sort by volume.
    Volume,
    /// Sort by trades.
    Trades,
    /// Sort by percent change up.
    PercentChangeUp,
    /// Sort by percent change down.
    PercentChangeDown,
}

impl fmt::Display for Sort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sort::Volume => write!(f, "VOLUME"),
            Sort::Trades => write!(f, "TRADES"),
            Sort::PercentChangeUp => write!(f, "PERCENT_CHANGE_UP"),
            Sort::PercentChangeDown => write!(f, "PERCENT_CHANGE_DOWN"),
        }
    }
}

/// Represents the projection type for instruments.
#[derive(Eq, PartialEq, Hash, Clone)]
pub enum Projection {
    /// Symbol search projection.
    SymbolSearch,
    /// Symbol regex projection.
    SymbolRegex,
    /// Description search projection.
    DescSearch,
    /// Description regex projection.
    DescRegex,
    /// Search projection.
    Search,
    /// Fundamental projection.
    Fundamental,
}

impl fmt::Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Projection::SymbolSearch => write!(f, "symbol-search"),
            Projection::SymbolRegex => write!(f, "symbol-regex"),
            Projection::DescSearch => write!(f, "desc-search"),
            Projection::DescRegex => write!(f, "desc-regex"),
            Projection::Search => write!(f, "search"),
            Projection::Fundamental => write!(f, "fundamental"),
        }
    }
}

/// Represents the market symbols for market hours.
#[derive(Eq, PartialEq, Hash, Clone)]
pub enum MarketSymbol {
    /// Equity market.
    Equity,
    /// Option market.
    Option,
    /// Bond market.
    Bond,
    /// Future market.
    Future,
    /// Forex market.
    Forex,
}

impl fmt::Display for MarketSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketSymbol::Equity => write!(f, "equity"),
            MarketSymbol::Option => write!(f, "option"),
            MarketSymbol::Bond => write!(f, "bond"),
            MarketSymbol::Future => write!(f, "future"),
            MarketSymbol::Forex => write!(f, "forex"),
        }
    }
}

/// A client for interacting with the Schwab API.
pub struct SchwabApi {
    reqwest_client: Arc<Client>,
    tokens_file_path: String,
}

impl SchwabApi {
    /// Creates a new `SchwabApi` instance.
    ///
    /// # Arguments
    ///
    /// * `reqwest_client` - An `Arc` wrapped `reqwest::Client` to be used for making HTTP requests.
    /// * `base_url` - The base URL for the Schwab API.
    /// * `tokens_file_path` - The path to the file where tokens are stored.
    ///
    /// # Returns
    ///
    /// A new `SchwabApi` instance.
    pub fn new(reqwest_client: Arc<Client>, tokens_file_path: String) -> Self {
        Self {
            reqwest_client,
            tokens_file_path,
        }
    }

    /// Constructs the request headers for a Schwab API request.
    ///
    /// # Returns
    ///
    /// A new `SchwabApi` instance.
    pub fn default() -> Self {
        Self {
            reqwest_client: Arc::new(Client::new()),
            tokens_file_path: TOKENS_FILE.to_owned(),
        }
    }

    /// Retrieves real-time quotes for a specified list of symbols.
    ///
    /// This method allows fetching various types of quote data (e.g., fundamental, extended)
    /// and can optionally return indicative quotes.
    ///
    /// # Arguments
    ///
    /// * `symbols` - A `Vec` of `String`s representing the ticker symbols for which to retrieve quotes.
    /// * `fields` - An `Option`al `Vec` of `QuoteFields` to specify the data fields to include in the response.
    ///              If `None`, default fields will be returned.
    /// * `indicative` - An `Option`al `bool` to request indicative quotes. Set to `true` for indicative quotes.
    ///                  If `None`, the default behavior of the API will be used.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the quote data, or an `anyhow::Error` if the request fails.
    pub async fn get_quotes(
        &self,
        symbols: Vec<String>,
        fields: Option<Vec<QuoteFields>>,
        indicative: Option<bool>,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let symbols_string = symbols.join(",");
        let fields_string = match fields {
            Some(v) => dedup_ordered(v)
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(","),
            None => "".to_owned(),
        };
        let indicative_string = match indicative {
            Some(v) => v.to_string().to_lowercase(),
            None => "".to_owned(),
        };

        let headers = self.construct_request_headers().await?;

        let request_url = format!(
            "{}/quotes?symbols={}&fields={}&indicative={}",
            SCHWAB_MARKET_DATA_API_URL, symbols_string, fields_string, indicative_string
        );
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    pub async fn get_preferences(&self) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;
        let response = self.reqwest_client
            .get(format!("{SCHWAB_TRADER_API_URL}/userPreference"))
            .headers(headers)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Gets an options chain for a symbol.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol to get the options chain for.
    /// * `contract_type` - The type of contract to get.
    /// * `strike_count` - The number of strikes to return.
    /// * `include_underlying_quote` - Whether to include the underlying quote in the response.
    /// Retrieves an options chain for a given symbol.
    ///
    /// This method allows specifying the contract type, the number of strikes, and whether to
    /// include the underlying quote in the response.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The ticker symbol for which to retrieve the options chain.
    /// * `contract_type` - The `ContractType` to filter the options chain (e.g., Call, Put, All).
    /// * `strike_count` - The number of strikes to return around the current price.
    /// * `include_underlying_quote` - A `bool` indicating whether to include the underlying stock's quote in the response.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the options chain data, or an `anyhow::Error` if the request fails.
    pub async fn get_chains(
        &self,
        symbol: String,
        contract_type: ContractType,
        strike_count: u64,
        include_underlying_quote: bool,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let request_url = format!(
            "{}/chains?symbol={}&contractType={}&strikeCount={}&includeUnderlyingQuote={}",
            SCHWAB_MARKET_DATA_API_URL,
            symbol,
            contract_type.to_string(),
            strike_count.to_string(),
            include_underlying_quote.to_string()
        );
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get quote for a single symbol.
    ///
    /// # Arguments
    ///
    /// * `symbol_id` - Ticker symbol.
    /// * `fields` - Fields to get ("all", "quote", "fundamental").
    /// Retrieves a quote for a single symbol.
    ///
    /// This method fetches detailed quote information for a specific ticker symbol,
    /// with options to specify the desired fields.
    ///
    /// # Arguments
    ///
    /// * `symbol_id` - The ticker symbol for which to retrieve the quote.
    /// * `fields` - An `Option`al `Vec` of `QuoteFields` to specify the data fields to include in the response.
    ///              If `None`, default fields will be returned.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the quote data, or an `anyhow::Error` if the request fails.
    pub async fn quote(
        &self,
        symbol_id: String,
        fields: Option<Vec<QuoteFields>>,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let fields_string = match fields {
            Some(v) => dedup_ordered(v)
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(","),
            None => "".to_owned(),
        };

        let params = parse_params(vec![("fields", Some(fields_string))]);

        let request_url = format!(
            "{}/{}/quotes",
            SCHWAB_MARKET_DATA_API_URL,
            encode(&symbol_id)
        );
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get an option expiration chain for a ticker.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Ticker symbol.
    /// Retrieves the option expiration chain for a given ticker symbol.
    ///
    /// This provides a list of available expiration dates for options contracts related to the symbol.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The ticker symbol for which to retrieve the option expiration chain.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the option expiration chain data, or an `anyhow::Error` if the request fails.
    pub async fn option_expiration_chain(
        &self,
        symbol: String,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let params = parse_params(vec![("symbol", Some(symbol))]);

        let request_url = format!("{}/expirationchain", SCHWAB_MARKET_DATA_API_URL);
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get price history for a ticker.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Ticker symbol.
    /// * `period_type` - Period type ("day"|"month"|"year"|"ytd").
    /// * `period` - Period.
    /// * `frequency_type` - Frequency type ("minute"|"daily"|"weekly"|"monthly").
    /// * `frequency` - Frequency (frequencyType: options), (minute: 1, 5, 10, 15, 30), (daily: 1), (weekly: 1), (monthly: 1).
    /// * `start_date` - Start date.
    /// * `end_date` - End date.
    /// * `need_extended_hours_data` - Need extended hours data (True|False).
    /// * `need_previous_close` - Need previous close (True|False).
    /// Retrieves historical price data (candle history) for a given ticker symbol.
    ///
    /// This method allows extensive customization of the historical data, including period type,
    /// frequency, start and end dates, and whether to include extended hours data.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The ticker symbol for which to retrieve price history.
    /// * `period_type` - An `Option`al `PeriodType` to specify the overall period of the data (e.g., Day, Month, Year).
    /// * `period` - An `Option`al `u64` representing the number of periods to retrieve.
    /// * `frequency_type` - An `Option`al `FrequencyType` to specify the granularity of the data (e.g., Minute, Daily, Weekly).
    /// * `frequency` - An `Option`al `u64` representing the number of frequency units per candle (e.g., 1, 5, 10 minutes).
    /// * `start_date` - An `Option`al `DateTime<Utc>` to specify the start date for the historical data.
    /// * `end_date` - An `Option`al `DateTime<Utc>` to specify the end date for the historical data.
    /// * `need_extended_hours_data` - An `Option`al `bool` to include extended hours trading data.
    /// * `need_previous_close` - An `Option`al `bool` to include the previous day's closing price.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the candle history data, or an `anyhow::Error` if the request fails.
    pub async fn price_history(
        &self,
        symbol: String,
        period_type: Option<PeriodType>,
        period: Option<u64>,
        frequency_type: Option<FrequencyType>,
        frequency: Option<u64>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        need_extended_hours_data: Option<bool>,
        need_previous_close: Option<bool>,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let params = parse_params(vec![
            ("symbol", Some(symbol)),
            ("periodType", period_type.map(|p| p.to_string())),
            ("period", period.map(|p| p.to_string())),
            ("frequencyType", frequency_type.map(|f| f.to_string())),
            ("frequency", frequency.map(|f| f.to_string())),
            ("startDate", time_to_epoch_ms(start_date)),
            ("endDate", time_to_epoch_ms(end_date)),
            (
                "needExtendedHoursData",
                need_extended_hours_data.map(|b| b.to_string()),
            ),
            (
                "needPreviousClose",
                need_previous_close.map(|b| b.to_string()),
            ),
        ]);

        let request_url = format!("{}/pricehistory", SCHWAB_MARKET_DATA_API_URL);
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get movers in a specific index and direction.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Symbol ("$DJI"|"$COMPX"|"$SPX"|"NYSE"|"NASDAQ"|"OTCBB"|"INDEX_ALL"|"EQUITY_ALL"|"OPTION_ALL"|"OPTION_PUT"|"OPTION_CALL").
    /// * `sort` - Sort ("VOLUME"|"TRADES"|"PERCENT_CHANGE_UP"|"PERCENT_CHANGE_DOWN").
    /// * `frequency` - Frequency (0|1|5|10|30|60).
    ///
    /// # Notes
    ///
    /// Must be called within market hours (there aren't really movers outside of market hours).
    /// Retrieves a list of top movers for a specific index and direction.
    ///
    /// This method provides insights into market activity by listing symbols that have
    /// experienced significant movement based on volume, trades, or percentage change.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The index symbol (e.g., "$DJI", "$COMPX", "$SPX") or market category (e.g., "NYSE", "NASDAQ").
    /// * `sort` - An `Option`al `Sort` order to specify how the movers should be sorted (e.g., Volume, PercentChangeUp).
    /// * `frequency` - An `Option`al `u64` representing the frequency of the data (e.g., 0, 1, 5, 10, 30, 60).
    ///
    /// # Notes
    ///
    /// This function is most relevant when called within market hours, as movers are typically
    /// determined by real-time trading activity.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the movers data, or an `anyhow::Error` if the request fails.
    pub async fn movers(
        &self,
        symbol: String,
        sort: Option<Sort>,
        frequency: Option<u64>,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let params = parse_params(vec![
            ("sort", sort.map(|s| s.to_string())),
            ("frequency", frequency.map(|f| f.to_string())),
        ]);

        let request_url = format!("{}/movers/{}", SCHWAB_MARKET_DATA_API_URL, encode(&symbol));
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get Market Hours for dates in the future across different markets.
    ///
    /// # Arguments
    ///
    /// * `symbols` - List of market symbols ("equity", "option", "bond", "future", "forex").
    /// * `date` - Date.
    /// Retrieves market hours for a list of specified market symbols on a given date.
    ///
    /// This method provides information about when various markets (e.g., equity, option, bond)
    /// are open or closed.
    ///
    /// # Arguments
    ///
    /// * `symbols` - A `Vec` of `MarketSymbol`s representing the markets for which to retrieve hours.
    /// * `date` - An `Option`al `DateTime<Utc>` to specify the date for which to retrieve market hours.
    ///            If `None`, the current date will be used.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the market hours data, or an `anyhow::Error` if the request fails.
    pub async fn market_hours(
        &self,
        symbols: Vec<MarketSymbol>,
        date: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let symbols_string = symbols
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .join(",");

        let params = parse_params(vec![
            ("markets", Some(symbols_string)),
            ("date", time_to_yyyymmdd(date)),
        ]);

        let request_url = format!("{}/markets", SCHWAB_MARKET_DATA_API_URL);
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get Market Hours for dates in the future for a single market.
    ///
    /// # Arguments
    ///
    /// * `market_id` - Market id ("equity"|"option"|"bond"|"future"|"forex").
    /// * `date` - Date.
    /// Retrieves market hours for a single specified market symbol on a given date.
    ///
    /// This method provides detailed information about the opening and closing times for a
    /// particular market.
    ///
    /// # Arguments
    ///
    /// * `market_id` - The `MarketSymbol` representing the specific market for which to retrieve hours.
    /// * `date` - An `Option`al `DateTime<Utc>` to specify the date for which to retrieve market hours.
    ///            If `None`, the current date will be used.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the market hours data, or an `anyhow::Error` if the request fails.
    pub async fn market_hour(
        &self,
        market_id: MarketSymbol,
        date: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let params = parse_params(vec![("date", time_to_yyyymmdd(date))]);

        let request_url = format!(
            "{}/markets/{}",
            SCHWAB_MARKET_DATA_API_URL,
            market_id.to_string()
        );
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get instruments for a list of symbols.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Symbol.
    /// * `projection` - Projection ("symbol-search"|"symbol-regex"|"desc-search"|"desc-regex"|"search"|"fundamental").
    /// Searches for instruments based on a symbol and projection type.
    ///
    /// This method allows finding instruments by symbol, description, or using regular expressions,
    /// and can return fundamental data.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol or description to search for.
    /// * `projection` - The `Projection` type to specify the search method and data to return.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the instrument data, or an `anyhow::Error` if the request fails.
    pub async fn instruments(
        &self,
        symbol: String,
        projection: Projection,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let params = parse_params(vec![
            ("symbol", Some(symbol)),
            ("projection", Some(projection.to_string())),
        ]);

        let request_url = format!("{}/instruments", SCHWAB_MARKET_DATA_API_URL);
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .query(&params)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Get instrument for a single cusip.
    ///
    /// # Arguments
    ///
    /// * `cusip_id` - Cusip id.
    /// Retrieves instrument details for a single CUSIP ID.
    ///
    /// This method provides specific information about an instrument identified by its CUSIP.
    ///
    /// # Arguments
    ///
    /// * `cusip_id` - The CUSIP ID of the instrument to retrieve.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the instrument data, or an `anyhow::Error` if the request fails.
    pub async fn instrument_cusip(&self, cusip_id: String) -> anyhow::Result<Value, anyhow::Error> {
        let headers = self.construct_request_headers().await?;

        let request_url = format!(
            "{}/instruments/{}",
            SCHWAB_MARKET_DATA_API_URL,
            encode(&cusip_id)
        );
        let response = self
            .reqwest_client
            .get(request_url)
            .headers(headers)
            .send()
            .await?;

        let response_json = serde_json::from_str(response.text().await?.as_str())?;
        Ok(response_json)
    }

    /// Constructs the request headers for a Schwab API request.
    ///
    /// # Returns
    ///
    /// A `HeaderMap` containing the required headers for a Schwab API request.
    async fn construct_request_headers(&self) -> anyhow::Result<HeaderMap, anyhow::Error> {
        let mut headers = HeaderMap::new();

        let json_string = tokio::fs::read_to_string(&self.tokens_file_path).await?;
        let data: StoredTokenInfo = serde_json::from_str(&json_string)?;
        let auth_header = format!("Bearer {}", data.access_token.as_str());

        headers.append("Accept", HeaderValue::from_str("application/json")?);
        headers.append(
            "Authorization",
            HeaderValue::from_str(auth_header.as_str())?,
        );

        Ok(headers)
    }
}
