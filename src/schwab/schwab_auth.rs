use std::{fs, io::{self, Write}, sync::Arc};

use base64::{engine::general_purpose, Engine};
use reqwest::{header::{HeaderMap, HeaderValue}, Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::schwab::common::TOKENS_FILE;

const SCHWAB_AUTH_URL: &str = "https://api.schwabapi.com/v1/oauth/authorize?response_type=code";
const SCHWAB_TOKEN_URL: &str = "https://api.schwabapi.com/v1/oauth/token";
const REDIRECT_URI: &str = "https://127.0.0.1";

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
    pub fn new(reqwest_client: Arc<Client>) -> Self {
        Self { reqwest_client }
    }

    pub fn default() -> Self {
        Self {
            reqwest_client: Arc::new(Client::new())
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
    /// * `app_key` - The application key.
    /// * `secret` - The application secret.
    ///
    /// # Returns
    ///
    /// An empty `Result` indicating success or failure.
    pub async fn authorize(
        &self,
        app_key: &str,
        secret: &str,
    ) -> anyhow::Result<()> {
        let full_auth_url = format!(
            "{}&client_id={}&scope=readonly&redirect_uri={}",
            SCHWAB_AUTH_URL, app_key, REDIRECT_URI
        );

        // Prompt the user to log in and authorize the application.
        println!("\nSchwab API Authorization Guide:");
        println!("1. Copy and paste the following URL into your browser:");
        println!("{}", full_auth_url);
        println!("2. Log in with your Schwab portfolio credentials and authorize the application.");
        println!("3. You will be redirected to an empty page. Copy the FULL URL from the address bar.");
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
        info!("Saving tokens to {}", TOKENS_FILE);
        fs::write(TOKENS_FILE, json_string)?;
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
    ///
    /// # Returns
    ///
    /// An empty `Result` indicating success or failure.
    pub async fn refresh_tokens(&self, app_key: &str, secret: &str) -> anyhow::Result<(), anyhow::Error> {
        let json_string = fs::read_to_string(TOKENS_FILE)?;
        let data: StoredTokenInfo = serde_json::from_str(&json_string)?;

        let refresh_token = data.refresh_token;
        let headers = self.construct_headers(app_key, secret);
        let payload = self.construct_refresh_payload(refresh_token);

        let refresh_tokens_response = self.reqwest_client
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
            fs::write(TOKENS_FILE, serde_json::to_string_pretty(&refresh_token_json)?)?;
        } else {
            info!("Failed to refresh tokens.");
            return Err(anyhow::anyhow!("Failed to refresh tokens."));
        }

        Ok(())
    }

    /// Extracts the authorization code from the URL string.
    fn extract_auth_code(&self, url: &str) -> anyhow::Result<String> {
        let code_start = url
            .find("code=")
            .ok_or_else(|| anyhow::anyhow!("'code=' not found in URL"))?;
        let code_end = url
            .find("&")
            .unwrap_or(url.len()); // Use end of string if no space character
        
        let code = url[code_start + 5..code_end].to_string();
        
        // The code ends with a special character, we must re-add the '@' which is encoded as %40
        let decoded_code = code.replace("%40", "@");
        Ok(decoded_code)
    }

    fn construct_headers(
        &self,
        app_key: &str,
        app_secret: &str,
    ) -> HeaderMap {
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

    /// Constructs the necessary headers and payload for the token exchange request.
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

    fn construct_refresh_payload(&self, refresh_token: String) -> RefreshRequestPayload {
        let payload = RefreshRequestPayload {
            grant_type: "refresh_token".to_owned(),
            refresh_token,
        };

        payload
    }

    /// Sends the token request to the Schwab API and returns the JSON response.
    async fn retrieve_tokens(
        &self,
        headers: HeaderMap,
        payload: AuthRequestPayload,
    ) -> anyhow::Result<Value> {
        // Send the POST request to the token URL.
        let init_token_response = self.reqwest_client
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


#[cfg(test)]
mod test {
    use std::sync::Arc;
    use anyhow::Result;

    use crate::{SchwabAuth};

    const SCHWAB_APP_KEY_ENV_VAR: &str = "SCHWAB_APP_KEY";
    const SCHWAB_APP_SECRET_ENV_VAR: &str = "SCHWAB_APP_SECRET";

    // NOTE: This test requires a valid 'tokens.json' file to exist in the
    // root directory of your project. This file is generated by a successful
    // authorization flow via the `SchwabAuth::authorize` method.
    #[tokio::test]
    async fn test_refresh_tokens() -> Result<()> {
        // Create an HTTP client shared across the API instance.
        let client = Arc::new(reqwest::Client::new());

        // Get the application key from environment variables.
        let client_app_key = std::env::var(SCHWAB_APP_KEY_ENV_VAR)
            .expect("SCHWAB_APP_KEY environment variable not set");

        // Get the application secret from environment variables.
        let client_secret = std::env::var(SCHWAB_APP_SECRET_ENV_VAR)
            .expect("SCHWAB_APP_SECRET environment variable not set");
        
        // Instantiate the SchwabAuth struct to access its methods.
        let schwab_auth = SchwabAuth::new(client);

        // Call the refresh_tokens method on the instantiated object.
        let res = schwab_auth.refresh_tokens(&client_app_key, &client_secret).await;
        
        // Assert that the operation was successful.
        // A successful refresh will return Ok(()), while a failure will return an Err.
        assert!(res.is_ok());

        Ok(())
    }
}
