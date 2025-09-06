use std::{
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

use brokerage_api::{
    SchwabApi, SchwabAuth, SchwabStreamer,
    schwab::schwab_streamer::{Command, Service, StreamRequest},
};
use chrono::Utc;
use reqwest::Client;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    let app_key = std::env::var("SCHWAB_APP_KEY")?;
    let app_secret = std::env::var("SCHWAB_APP_SECRET")?;

    let schwab_auth = SchwabAuth::default();

    // schwab_auth.authorize(&app_key, &app_secret).await?;
    schwab_auth.refresh_tokens(&app_key, &app_secret).await?;

    let mut streamer_api = SchwabStreamer::default().await?;
    // TODO: send_requests should queue up the requests using record_request method, and there should be an async handler that runs periodically
    // to send all queued up requests. This way the client doesn't have to do the management of knowing when streamer is_active vs not, to decide
    // if it's safe to send a request.
    let mut request_sent = false;
    while request_sent == false {
        if !streamer_api.is_active() {
            continue;
        }
        println!("[{:?}] Login succeeded", Utc::now());
        // println!("[{:?}] Sleeping for 15 seconds", Utc::now());
        // sleep(Duration::from_secs(15));
        if request_sent == false {
            println!("[{:?}] Sending SUBS request", Utc::now());
            let stream_requests = vec![streamer_api.level_one_equities(
                vec!["NVDA".to_owned()],
                vec![],
                Command::Subs,
            )?];
            streamer_api.send_requests(stream_requests).await?;
            request_sent = true;
        }
    }
    loop {}
}
