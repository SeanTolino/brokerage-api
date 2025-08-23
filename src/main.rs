use std::{sync::Arc, thread::sleep, time::Duration};

use brokerage_api::{schwab::schwab_streamer_api::{Command, Service, StreamRequest}, SchwabApi, SchwabAuth, SchwabStreamerApi};
use reqwest::Client;


#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    let reqwest_client = Arc::new(Client::new());
    let schwab_auth = SchwabAuth::new(Arc::clone(&reqwest_client));
    let schwab_api = SchwabApi::new(Arc::clone(&reqwest_client));

    let app_key = std::env::var("SCHWAB_APP_KEY")?;
    let app_secret = std::env::var("SCHWAB_APP_SECRET")?;
    // schwab_auth.authorize(&app_key, &app_secret).await?;
    schwab_auth.refresh_tokens(&app_key, &app_secret).await?;
    
    let mut streamer_api = SchwabStreamerApi::new(schwab_api).await?;
    streamer_api.start().await?;
    loop {
        let stream_requests = vec![
          StreamRequest::new(Service::LevelOneEquities, Command::Subs, vec!["AAPL".to_owned()], vec![])
        ];
        streamer_api.send_requests(stream_requests).await?;
        sleep(Duration::from_secs(5));
    }
}
