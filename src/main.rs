use std::sync::Arc;

use brokerage_api::{SchwabStreamerApi, SchwabApi, SchwabAuth};
use reqwest::Client;


#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    let reqwest_client = Arc::new(Client::new());
    let schwab_auth = SchwabAuth::new(Arc::clone(&reqwest_client));
    let schwab_api = SchwabApi::new(Arc::clone(&reqwest_client));
    let streamer_api = SchwabStreamerApi::new(schwab_api);

    let app_key = std::env::var("SCHWAB_APP_KEY")?;
    let app_secret = std::env::var("SCHWAB_APP_SECRET")?;
    schwab_auth.refresh_tokens(&app_key, &app_secret).await?;
    
    let _res = streamer_api.conn().await;
    Ok(())
}
