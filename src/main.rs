use brokerage_api::{
    SchwabAuth, SchwabStreamer,
    schwab::{models::streamer::StreamerMessage, schwab_streamer::Command},
};

#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let streamer = SchwabStreamer::default().await?;

    // Start the streamer and get the receiver for messages
    let mut receiver = streamer.start().await?;

    // Wait for the streamer to log in and become active
    while !streamer.is_active().await {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    println!("Streamer is active. Subscribing to SPY quotes.");

    // 1. Build a typed request. This does NOT send anything.
    let equity_request = streamer.level_one_equities(
        vec!["SPY".to_string()],
        vec![], // Empty vec for all fields
        Command::Subs,
    );

    // 2. Send the request and update the internal state.
    streamer.send(vec![equity_request]).await?;

    println!("Subscription sent. Waiting for messages...");

    // 3. Listen for incoming messages on the channel
    while let Some(message) = receiver.recv().await {
        match message {
            StreamerMessage::LevelOneEquity(equity_quote) => {
                println!(
                    "Received Equity Quote for {}: Bid: {:?}, Ask: {:?}",
                    equity_quote.symbol, equity_quote.bid_price, equity_quote.ask_price
                );
            }
            StreamerMessage::LevelOneOption(option_quote) => {
                println!(
                    "Received Option Quote for {}: Mark Price: {:?}",
                    option_quote.symbol, option_quote.mark_price
                );
            }
        }
    }

    loop {}
}
