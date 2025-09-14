use std::{
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

use brokerage_api::{
    schwab::{models::streamer::StreamerMessage, schwab_streamer::{self, Command, Service, StreamRequest}}, SchwabApi, SchwabAuth, SchwabStreamer
};
use chrono::Utc;
use reqwest::Client;
use tokio::sync::Mutex;
use tracing::{info, Level};
use tracing_subscriber::fmt::init;

#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let mut streamer = SchwabStreamer::default().await?;
    
    // Start the streamer and get the receiver for messages
    let mut receiver = streamer.start().await?;
    
    // Wait for the streamer to log in and become active
    while !streamer.is_active().await {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    println!("Streamer is active. Subscribing to SPY quotes.");

    // Build and send a subscription request
    let equity_request = streamer.level_one_equities(
        vec!["SPY".to_string()], 
        vec![], // Empty vec for all fields
        schwab_streamer::Command::Subs,
    )?;
    streamer.send_requests(vec![equity_request]).await?;

    println!("Subscription sent. Waiting for messages...");

    // Listen for incoming messages on the channel
    while let Some(message) = receiver.recv().await {
        match message {
            StreamerMessage::LevelOneEquity(equity_quote) => {
                println!(
                    "Received Equity Quote for {}: Bid: {:?}, Ask: {:?}",
                    equity_quote.symbol,
                    equity_quote.bid_price,
                    equity_quote.ask_price
                );
            }
            StreamerMessage::LevelOneOption(option_quote) => {
                println!(
                    "Received Option Quote for {}: Mark Price: {:?}",
                    option_quote.symbol,
                    option_quote.mark_price
                );
            }
        }
    }

    loop {}
}
