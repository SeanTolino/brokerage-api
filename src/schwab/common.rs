/// The file name where authentication tokens are stored.
pub(crate) const TOKENS_FILE: &str = "tokens.json";
/// The base URL for the Schwab Market Data API.
pub(crate) const SCHWAB_MARKET_DATA_API_URL: &str = "https://api.schwabapi.com/marketdata/v1";
/// The base URL for Schwab API authorization.
pub(crate) const SCHWAB_AUTH_URL: &str = "https://api.schwabapi.com/v1/oauth/authorize?response_type=code";
/// The URL for exchanging authorization codes or refresh tokens for access tokens.
pub(crate) const SCHWAB_TOKEN_URL: &str = "https://api.schwabapi.com/v1/oauth/token";
/// The redirect URI used during the OAuth 2.0 authorization flow.
pub(crate) const REDIRECT_URI: &str = "https://127.0.0.1";
/// The redirect URI used during the OAuth 2.0 authorization flow.
/// TODO:
pub(crate) const SCHWAB_TRADER_API_URL: &str = "https://api.schwabapi.com/trader/v1";
/// TODO:
pub(crate) const SCHWAB_STREAMER_API_URL: &str = "wss://streamer-api.schwab.com/ws";
