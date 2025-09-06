use std::{
    fs,
    io::{self, Write},
    sync::Arc,
};

use base64::{Engine, engine::general_purpose};
use reqwest::{
    Client,
    header::{HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::schwab::common::{REDIRECT_URI, SCHWAB_AUTH_URL, SCHWAB_TOKEN_URL, TOKENS_FILE};

#[derive(Serialize, Debug)]
struct AuthRequestPayload {
    grant_type: String,
    code: String,
    redirect_uri: String,
}

#[derive(Serialize, Debug)]
struct RefreshRequestPayload {
    grant_type: String,
    refresh_token: String,
}

/// Represents the token information stored in a local file.
#[derive(Serialize, Deserialize, Debug)]
#[allow(dead_code)]
pub(crate) struct StoredTokenInfo {
    /// The access token.
    pub(crate) access_token: String,
    /// The number of seconds until the access token expires.
    pub(crate) expires_in: u64,
    /// The ID token.
    pub(crate) id_token: String,
    /// The refresh token.
    pub(crate) refresh_token: String,
    /// The scope of the access token.
    pub(crate) scope: String,
    /// The type of the token.
    pub(crate) token_type: String,
}

/// A client for handling the Schwab API authentication process.
#[derive(Clone)]
pub struct SchwabAuth {
    reqwest_client: Arc<Client>,
    tokens_file_path: String,
}

impl SchwabAuth {
    /// Creates a new `SchwabAuth` instance.
    ///
    /// # Arguments
    ///
    /// * `reqwest_client` - An `Arc` wrapped `reqwest::Client` to be used for making HTTP requests.
    ///
    /// # Returns
    ///
    /// A new `SchwabAuth` instance.
    pub fn new(reqwest_client: Arc<Client>, tokens_file_path: String) -> Self {
        Self {
            reqwest_client,
            tokens_file_path,
        }
    }

    /// Creates a new `SchwabAuth` instance with default settings.
    ///
    /// This uses a default `reqwest::Client` and the default `TOKENS_FILE` path.
    ///
    /// # Returns
    ///
    /// A new `SchwabAuth` instance.
    pub fn default() -> Self {
        Self {
            reqwest_client: Arc::new(Client::new()),
            tokens_file_path: TOKENS_FILE.to_owned(),
        }
    }

    /// Guides the user through the Schwab API authorization process.
    ///
    /// This method constructs the authorization URL, prompts the user to log in and authorize the application,
    /// and then exchanges the authorization code for an access token and refresh token. The tokens are then
    /// saved to a local file.
    ///
    /// # Arguments
    ///
    /// * `app_key` - The application key (Client ID) provided by Schwab.
    /// * `secret` - The application secret (Client Secret) provided by Schwab.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success (`Ok(())`) or an `anyhow::Error` if the authorization process fails.
    pub async fn authorize(&self, app_key: &str, secret: &str) -> anyhow::Result<()> {
        let full_auth_url = format!(
            "{}&client_id={}&scope=readonly&redirect_uri={}",
            SCHWAB_AUTH_URL, app_key, REDIRECT_URI
        );

        // Prompt the user to log in and authorize the application.
        println!("\nSchwab API Authorization Guide:");
        println!("1. Copy and paste the following URL into your browser:");
        println!("{}", full_auth_url);
        println!("2. Log in with your Schwab portfolio credentials and authorize the application.");
        println!(
            "3. You will be redirected to an empty page. Copy the FULL URL from the address bar."
        );
        print!("4. Paste the URL here and press Enter: ");
        io::stdout().flush()?; // Ensure the prompt is displayed immediately.

        let mut returned_url = String::new();
        io::stdin().read_line(&mut returned_url)?;

        // Extract the authorization code from the returned URL.
        let response_code = self.extract_auth_code(&returned_url)?;
        info!("Successfully extracted response code: {}", response_code);

        // Construct headers and payload for the token request.
        let headers = self.construct_headers(app_key, secret);
        let payload = self.construct_auth_payload(REDIRECT_URI, &response_code);
        info!("Constructed headers and payload.");

        // Retrieve the tokens using the authorization code.
        let token_response_body = self.retrieve_tokens(headers, payload).await?;
        info!("Successfully retrieved tokens from API.");

        // Convert the token response to a JSON string.
        let json_string = serde_json::to_string_pretty(&token_response_body)?;

        // Save the tokens to a local file.
        info!("Saving tokens to {}", self.tokens_file_path);
        fs::write(self.tokens_file_path.to_owned(), json_string)?;
        info!("Tokens saved successfully!");

        Ok(())
    }

    /// Refreshes the access token using the refresh token.
    ///
    /// This method reads the refresh token from the local tokens file, and then uses it to request a new
    /// access token. The new tokens are then saved to the local file.
    ///
    /// # Arguments
    ///
    /// * `app_key` - The application key.
    /// * `secret` - The application secret.
    /// Refreshes the access token using the stored refresh token.
    ///
    /// This method reads the refresh token from the local tokens file, and then uses it to request a new
    /// access token and potentially a new refresh token. The newly obtained tokens are then saved to the local file.
    ///
    /// # Arguments
    ///
    /// * `app_key` - The application key (Client ID) provided by Schwab.
    /// * `secret` - The application secret (Client Secret) provided by Schwab.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success (`Ok(())`) or an `anyhow::Error` if the token refresh process fails.
    pub async fn refresh_tokens(
        &self,
        app_key: &str,
        secret: &str,
    ) -> anyhow::Result<(), anyhow::Error> {
        let json_string = fs::read_to_string(self.tokens_file_path.to_owned())?;
        let data: StoredTokenInfo = serde_json::from_str(&json_string)?;

        let refresh_token = data.refresh_token;
        let headers = self.construct_headers(app_key, secret);
        let payload = self.construct_refresh_payload(refresh_token);

        let refresh_tokens_response = self
            .reqwest_client
            .post(SCHWAB_TOKEN_URL)
            .headers(headers)
            .form(&payload)
            .send()
            .await?;

        info!("Refresh tokens response: {:?}", refresh_tokens_response);

        if refresh_tokens_response.status() == 200 {
            info!("Retrieved new tokens successfully using refresh token.");
            let refresh_token_string = refresh_tokens_response.text().await?;
            let refresh_token_json: StoredTokenInfo = serde_json::from_str(&refresh_token_string)?;
            fs::write(
                self.tokens_file_path.to_owned(),
                serde_json::to_string_pretty(&refresh_token_json)?,
            )?;
        } else {
            info!("Failed to refresh tokens.");
            return Err(anyhow::anyhow!("Failed to refresh tokens."));
        }

        Ok(())
    }

    /// Extracts the authorization code from the redirect URL returned by the Schwab authorization server.
    ///
    /// # Arguments
    ///
    /// * `url` - The full URL string received after the user authorizes the application.
    ///
    /// # Returns
    ///
    /// A `Result` containing the extracted authorization code as a `String`, or an `anyhow::Error` if the code cannot be found.
    fn extract_auth_code(&self, url: &str) -> anyhow::Result<String> {
        let code_start = url
            .find("code=")
            .ok_or_else(|| anyhow::anyhow!("'code=' not found in URL"))?;
        let code_end = url.find("&").unwrap_or(url.len()); // Use end of string if no space character

        let code = url[code_start + 5..code_end].to_string();

        // The code ends with a special character, we must re-add the '@' which is encoded as %40
        let decoded_code = code.replace("%40", "@");
        Ok(decoded_code)
    }

    /// Constructs the necessary `HeaderMap` for authentication requests.
    ///
    /// This method creates the `Authorization` header by base64 encoding the `app_key` and `app_secret`.
    ///
    /// # Arguments
    ///
    /// * `app_key` - The application key (Client ID).
    /// * `app_secret` - The application secret (Client Secret).
    ///
    /// # Returns
    ///
    /// A `HeaderMap` containing the `Authorization` and `Content-Type` headers.
    fn construct_headers(&self, app_key: &str, app_secret: &str) -> HeaderMap {
        // Combine key and secret for base64 encoding.
        let creds = format!("{}:{}", app_key, app_secret);

        // Perform base64 encoding.
        let mut encoded_credentials = String::new();
        general_purpose::STANDARD.encode_string(creds.as_bytes(), &mut encoded_credentials);

        // Create the headers map.
        let mut headers = HeaderMap::new();
        // Add the correct Authorization header with the encoded credentials.
        headers.append(
            "Authorization",
            HeaderValue::from_str(format!("Basic {}", encoded_credentials).as_str()).unwrap(),
        );
        // Set the content type.
        headers.append(
            "Content-Type",
            HeaderValue::from_str("application/x-www-form-urlencoded").unwrap(),
        );

        headers
    }

    /// Constructs the payload for the initial token exchange request (authorization code grant type).
    ///
    /// # Arguments
    ///
    /// * `redirect_uri` - The redirect URI used in the authorization request.
    /// * `response_code` - The authorization code received from the Schwab authorization server.
    ///
    /// # Returns
    ///
    /// An `AuthRequestPayload` struct containing the necessary parameters for the token request.
    fn construct_auth_payload(
        &self,
        redirect_uri: &str,
        response_code: &str,
    ) -> AuthRequestPayload {
        // Create the request payload.
        let payload = AuthRequestPayload {
            grant_type: "authorization_code".to_owned(),
            code: response_code.to_owned(),
            redirect_uri: redirect_uri.to_owned(),
        };

        payload
    }

    /// Constructs the payload for the token refresh request.
    ///
    /// # Arguments
    ///
    /// * `refresh_token` - The refresh token obtained during the initial authorization.
    ///
    /// # Returns
    ///
    /// A `RefreshRequestPayload` struct containing the necessary parameters for the refresh token request.
    fn construct_refresh_payload(&self, refresh_token: String) -> RefreshRequestPayload {
        let payload = RefreshRequestPayload {
            grant_type: "refresh_token".to_owned(),
            refresh_token,
        };

        payload
    }

    /// Sends the initial token request to the Schwab API and returns the JSON response.
    ///
    /// This method is used to exchange the authorization code for access and refresh tokens.
    ///
    /// # Arguments
    ///
    /// * `headers` - The `HeaderMap` containing the `Authorization` and `Content-Type` headers.
    /// * `payload` - The `AuthRequestPayload` containing the authorization code and redirect URI.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the token response, or an `anyhow::Error` if the request fails.
    async fn retrieve_tokens(
        &self,
        headers: HeaderMap,
        payload: AuthRequestPayload,
    ) -> anyhow::Result<Value> {
        // Send the POST request to the token URL.
        let init_token_response = self
            .reqwest_client
            .post(SCHWAB_TOKEN_URL)
            .headers(headers)
            .form(&payload) // Use .form() for URL-encoded data
            .send()
            .await?;

        info!("Response: {:?}", init_token_response);

        // Check if the request was successful.
        if !init_token_response.status().is_success() {
            let error_text = init_token_response.text().await?;
            return Err(anyhow::anyhow!("Failed to retrieve tokens: {}", error_text));
        }

        // Parse the JSON response.
        let json_response: Value = init_token_response.json().await?;
        Ok(json_response)
    }
}
