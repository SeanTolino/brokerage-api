use std::{sync::Arc, thread::sleep, time::Duration};

use brokerage_api::{schwab::schwab_streamer::{Command, Service, StreamRequest}, SchwabApi, SchwabAuth, SchwabStreamer};
use reqwest::Client;


#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    let app_key = std::env::var("SCHWAB_APP_KEY")?;
    let app_secret = std::env::var("SCHWAB_APP_SECRET")?;

    let schwab_auth = SchwabAuth::default();

    // schwab_auth.authorize(&app_key, &app_secret).await?;
    schwab_auth.refresh_tokens(&app_key, &app_secret).await?;
    
    let mut streamer_api = SchwabStreamer::default().await?;
    streamer_api.start().await?;
    // TODO: send_requests should queue up the requests using record_request method, and there should be an async handler that runs periodically
    // to send all queued up requests. This way the client doesn't have to do the management of knowing when streamer is_active vs not, to decide
    // if it's safe to send a request.
    let mut request_sent = false;
    loop {
        if !streamer_api.is_active() {
            println!("is_active is false, continuing");
            continue;
        }
        if request_sent == false {
            println!("SENDING REQUEST!");
            let stream_requests = vec![
                StreamRequest::new(Service::LevelOneEquities, Command::Subs, vec!["AMZN".to_owned()], vec![])
            ];
            streamer_api.send_requests(stream_requests).await?;
            request_sent = true;
        }
    }
}
