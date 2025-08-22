use std::{fs, str::FromStr};

use anyhow::anyhow;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::{client::IntoClientRequest, http::Uri, ClientRequestBuilder, Message}};
use futures_util::{StreamExt, SinkExt};

use crate::{schwab::{common::{SCHWAB_STREAMER_API_URL, TOKENS_FILE}, schwab_auth::StoredTokenInfo}, SchwabApi};

pub struct SchwabStreamerApi {
    schwab_api: SchwabApi,
    request_id: i64,
    streamer_info: Value,
}

impl SchwabStreamerApi {
    pub async fn new(schwab_api: SchwabApi) -> anyhow::Result<Self, anyhow::Error> {
        let user_preferences = schwab_api.get_preferences().await?;
        let streamer_info = serde_json::from_str::<Value>(&user_preferences.as_str())?;
        let streamer_info = match streamer_info.get("streamerInfo").and_then(|info| info.get(0)) {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        Ok(Self { 
            schwab_api,
            request_id: 0,
            streamer_info: streamer_info.clone(),
        })
    }

    pub async fn connect(&mut self) -> anyhow::Result<(), anyhow::Error> {
        let schwab_client_channel = match self.streamer_info.get("schwabClientChannel") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let schwab_client_function_id = match self.streamer_info.get("schwabClientFunctionId") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let json_string = fs::read_to_string(TOKENS_FILE)?;
        let data: StoredTokenInfo = serde_json::from_str(&json_string)?;
        let auth_header = format!("{}", data.access_token.as_str());

        let request = ClientRequestBuilder::new(Uri::from_str(SCHWAB_STREAMER_API_URL)?);
        let (ws_stream, response) = 
            connect_async(SCHWAB_STREAMER_API_URL)
            .await
            .expect("Failed to connect to stream API");

        let (mut write, mut read) = ws_stream.split();
        let parameters = json!({
            "Authorization": auth_header,
            "SchwabClientChannel": schwab_client_channel,
            "SchwabClientFunctionId": schwab_client_function_id,
        });
        let message = self.build_message("ADMIN", "LOGIN", parameters)?;
        println!("MESSAGE ===> {:?}", message);
        write.send(Message::Text(message.to_string().into())).await?;

        println!("sent");

        let read_future = read.for_each(|message| async {
            println!("receiving...");
            let data = message.unwrap().into_data();
            tokio::io::stdout().write(&data).await.unwrap();
            println!("received...");
        });

        read_future.await;

        Ok(())
    }

    fn build_message(&mut self, service: &str, command: &str, parameters: Value) -> anyhow::Result<Value, anyhow::Error> {
        let schwab_client_customer_id = match self.streamer_info.get("schwabClientCustomerId") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let schwab_client_correlation_id = match self.streamer_info.get("schwabClientCorrelId") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        self.request_id += 1;
        Ok(json!({
            "service": service,
            "command": command,
            "requestid": self.request_id,
            "parameters": parameters,
            "SchwabClientCustomerId": schwab_client_customer_id,
            "SchwabClientCorrelId": schwab_client_correlation_id,
        }))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use reqwest::Client;

    use crate::{schwab::schwab_streamer_api::SchwabStreamerApi, SchwabApi, SchwabAuth};


    #[tokio::test]
    async fn test_conn() -> anyhow::Result<(), anyhow::Error> {
        let reqwest_client = Arc::new(Client::new());
        let schwab_auth = SchwabAuth::new(Arc::clone(&reqwest_client));
        let schwab_api = SchwabApi::new(Arc::clone(&reqwest_client));
        let mut streamer_api = SchwabStreamerApi::new(schwab_api).await?;

        let app_key = std::env::var("SCHWAB_APP_KEY")?;
        let app_secret = std::env::var("SCHWAB_APP_SECRET")?;
        schwab_auth.authorize(&app_key, &app_secret).await?;
        
        let _res = streamer_api.connect().await?;
        Ok(())
    }
}
