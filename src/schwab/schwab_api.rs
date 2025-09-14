use std::{env, fmt, sync::Arc};

use chrono::{DateTime, Utc};
use reqwest::{header::HeaderMap, Client, RequestBuilder, Response, StatusCode};
use tokio::sync::Mutex;
use tracing::info;
use urlencoding::encode;

use crate::{
    schwab::{
        common::{SCHWAB_MARKET_DATA_API_URL, SCHWAB_TRADER_API_URL, TOKENS_FILE},
        models::{
            market_data::{
                ChainsResponse, ExpirationChainResponse, InstrumentsResponse, MarketHours,
                MarketHoursResponse, MoversResponse, PriceHistoryResponse, QuotesResponse,
            },
            trader::UserPreferencesResponse,
        },
        schwab_auth::{SchwabAuth, StoredTokenInfo},
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

/// A client for interacting with the Schwab API, with automatic token refreshing.
#[derive(Debug, Clone)]
pub struct SchwabApi {
    reqwest_client: Arc<Client>,
    app_key: String,
    app_secret: String,
    tokens_file_path: String,
    auth: SchwabAuth,
    token_info: Arc<Mutex<StoredTokenInfo>>,
}

impl SchwabApi {
    /// Creates a new `SchwabApi` instance.
    ///
    /// # Arguments
    /// * `app_key` - Your Schwab application key (Client ID).
    /// * `app_secret` - Your Schwab application secret (Client Secret).
    /// * `tokens_file_path` - The path to where the `tokens.json` file is stored.
    pub async fn new(
        app_key: String,
        app_secret: String,
        tokens_file_path: String,
    ) -> anyhow::Result<Self> {
        let reqwest_client = Arc::new(Client::new());
        let auth = SchwabAuth::new(reqwest_client.clone(), tokens_file_path.clone());

        let json_string = tokio::fs::read_to_string(&tokens_file_path).await?;
        let token_info: StoredTokenInfo = serde_json::from_str(&json_string)?;

        Ok(Self {
            reqwest_client,
            app_key,
            app_secret,
            tokens_file_path,
            auth,
            token_info: Arc::new(Mutex::new(token_info)),
        })
    }

    /// Creates a new `SchwabApi` instance with default settings, loading credentials from environment variables.
    /// It expects `SCHWAB_APP_KEY` and `SCHWAB_APP_SECRET` to be set.
    pub async fn default() -> anyhow::Result<Self> {
        let app_key = env::var("SCHWAB_APP_KEY")
            .map_err(|_| anyhow::anyhow!("SCHWAB_APP_KEY environment variable not set"))?;
        let app_secret = env::var("SCHWAB_APP_SECRET")
            .map_err(|_| anyhow::anyhow!("SCHWAB_APP_SECRET environment variable not set"))?;
        Self::new(app_key, app_secret, TOKENS_FILE.to_owned()).await
    }

    /// Centralized request sender that handles authentication and token refreshing.
    async fn send_request(&self, mut builder: RequestBuilder) -> anyhow::Result<Response> {
        // Sign the request with the current access token from memory
        let headers = self.construct_request_headers().await?;
        builder = builder.headers(headers);

        // Clone the request before sending, so we can retry it if the token is expired
        let retry_builder = builder
            .try_clone()
            .ok_or_else(|| anyhow::anyhow!("Failed to clone request for potential retry"))?;

        // Send the initial request
        let response = builder.send().await?;

        // Check if the token expired (401 Unauthorized)
        if response.status() == StatusCode::UNAUTHORIZED {
            info!("Token expired. Attempting to refresh...");
            self.refresh_and_store_token().await?;

            // Re-sign the cloned request with the new token
            let retry_headers = self.construct_request_headers().await?;
            let response = retry_builder.headers(retry_headers).send().await?;

            info!("Request successful after token refresh.");
            return Ok(response);
        }

        Ok(response)
    }

    /// Refreshes the token, updates the in-memory store, and writes the new token to the file.
    async fn refresh_and_store_token(&self) -> anyhow::Result<()> {
        let refresh_token = {
            let token_data = self.token_info.lock().await;
            token_data.refresh_token.clone()
        };

        let new_token_info = self
            .auth
            .refresh_tokens(&self.app_key, &self.app_secret, &refresh_token)
            .await?;

        // Update the in-memory token
        {
            let mut token_data = self.token_info.lock().await;
            *token_data = new_token_info.clone();
        }

        // Persist the new token to the file for future sessions
        let json_string = serde_json::to_string_pretty(&new_token_info)?;
        tokio::fs::write(&self.tokens_file_path, json_string).await?;
        info!("Successfully refreshed and stored new token.");

        Ok(())
    }

    /// Constructs the request headers from the in-memory token.
    async fn construct_request_headers(&self) -> anyhow::Result<HeaderMap> {
        let mut headers = HeaderMap::new();

        let token_data = self.token_info.lock().await;
        let auth_header = format!("Bearer {}", token_data.access_token);
        headers.insert("Authorization", auth_header.parse()?);

        Ok(headers)
    }

    pub async fn get_preferences(&self) -> anyhow::Result<UserPreferencesResponse> {
        let builder = self
            .reqwest_client
            .get(format!("{SCHWAB_TRADER_API_URL}/userPreference"));

        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn get_quotes(
        &self,
        symbols: Vec<String>,
        fields: Option<Vec<QuoteFields>>,
        indicative: Option<bool>,
    ) -> anyhow::Result<QuotesResponse> {
        let url = format!("{}/quotes", SCHWAB_MARKET_DATA_API_URL);

        let params = parse_params(vec![
            ("symbols", Some(symbols.join(","))),
            (
                "fields",
                fields.map(|v| {
                    dedup_ordered(v)
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<String>>()
                        .join(",")
                }),
            ),
            ("indicative", indicative.map(|v| v.to_string().to_lowercase())),
        ]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn get_chains(
        &self,
        symbol: String,
        contract_type: ContractType,
        strike_count: u64,
        include_underlying_quote: bool,
    ) -> anyhow::Result<ChainsResponse> {
        let url = format!("{}/chains", SCHWAB_MARKET_DATA_API_URL);

        let params = parse_params(vec![
            ("symbol", Some(symbol)),
            ("contractType", Some(contract_type.to_string())),
            ("strikeCount", Some(strike_count.to_string())),
            (
                "includeUnderlyingQuote",
                Some(include_underlying_quote.to_string()),
            ),
        ]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn quote(
        &self,
        symbol_id: String,
        fields: Option<Vec<QuoteFields>>,
    ) -> anyhow::Result<QuotesResponse> {
        let url = format!(
            "{}/{}/quotes",
            SCHWAB_MARKET_DATA_API_URL,
            encode(&symbol_id)
        );

        let params = parse_params(vec![(
            "fields",
            fields.map(|v| {
                dedup_ordered(v)
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            }),
        )]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn option_expiration_chain(
        &self,
        symbol: String,
    ) -> anyhow::Result<ExpirationChainResponse> {
        let url = format!("{}/expirationchain", SCHWAB_MARKET_DATA_API_URL);
        let params = parse_params(vec![("symbol", Some(symbol))]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

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
    ) -> anyhow::Result<PriceHistoryResponse> {
        let url = format!("{}/pricehistory", SCHWAB_MARKET_DATA_API_URL);

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

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn movers(
        &self,
        symbol: String,
        sort: Option<Sort>,
        frequency: Option<u64>,
    ) -> anyhow::Result<MoversResponse> {
        let url = format!("{}/movers/{}", SCHWAB_MARKET_DATA_API_URL, encode(&symbol));
        let params = parse_params(vec![
            ("sort", sort.map(|s| s.to_string())),
            ("frequency", frequency.map(|f| f.to_string())),
        ]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn market_hours(
        &self,
        symbols: Vec<MarketSymbol>,
        date: Option<DateTime<Utc>>,
    ) -> anyhow::Result<MarketHoursResponse> {
        let url = format!("{}/markets", SCHWAB_MARKET_DATA_API_URL);

        let symbols_string = symbols
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .join(",");

        let params = parse_params(vec![
            ("markets", Some(symbols_string)),
            ("date", time_to_yyyymmdd(date)),
        ]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn market_hour(
        &self,
        market_id: MarketSymbol,
        date: Option<DateTime<Utc>>,
    ) -> anyhow::Result<MarketHours> {
        let url = format!(
            "{}/markets/{}",
            SCHWAB_MARKET_DATA_API_URL,
            market_id.to_string()
        );

        let params = parse_params(vec![("date", time_to_yyyymmdd(date))]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;

        // The API wraps the single response in a map with the market name as the key.
        // We find the first value in the map and return it.
        let mut response_map: MarketHoursResponse = response.json().await?;
        let market_hours = response_map
            .into_values()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Market hours response was empty"))?;
        Ok(market_hours)
    }

    pub async fn instruments(
        &self,
        symbol: String,
        projection: Projection,
    ) -> anyhow::Result<InstrumentsResponse> {
        let url = format!("{}/instruments", SCHWAB_MARKET_DATA_API_URL);

        let params = parse_params(vec![
            ("symbol", Some(symbol)),
            ("projection", Some(projection.to_string())),
        ]);

        let builder = self.reqwest_client.get(url).query(&params);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub async fn instrument_cusip(&self, cusip_id: String) -> anyhow::Result<InstrumentsResponse> {
        let url = format!(
            "{}/instruments/{}",
            SCHWAB_MARKET_DATA_API_URL,
            encode(&cusip_id)
        );

        let builder = self.reqwest_client.get(url);
        let response = self.send_request(builder).await?;
        response.json().await.map_err(Into::into)
    }

    pub(crate) async fn token_info(&self) -> StoredTokenInfo {
        self.token_info.lock().await.clone()
    }
}