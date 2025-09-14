# Brokerage-API

[![Crates.io](https://img.shields.io/crates/v/brokerage-api.svg)](https://crates.io/crates/brokerage-api)
[![Docs.rs](https://docs.rs/brokerage-api/badge.svg)](https://docs.rs/brokerage-api)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/SeanTolino/brokerage-api)

An unofficial, asynchronous Rust client for the Charles Schwab brokerage API, designed for both stock and options data.

This library provides strongly-typed models for API responses and a robust client that handles authentication, token refreshing, and real-time data streaming, all built on the `tokio` asynchronous runtime.

## ✨ Features

* **Asynchronous:** Built with `async/.await` and `tokio` for high performance.
* **Strongly-Typed:** Clean, and easy-to-use data models for all API responses. No manual JSON parsing required.
* **Automatic Token Refresh:** The client transparently handles OAuth 2.0 token expiration and refreshing, so you don't have to.
* **Real-Time Data Streaming:** 📈 A WebSocket streamer provides live market data through a simple channel-based interface.
* **State Management:** The streamer tracks its own subscriptions, paving the way for automatic resubscription on reconnect.

---

## ⚠️ Disclaimer & Current Status

This is an **unofficial** library and is not affiliated with, endorsed, or supported by Charles Schwab in any way. Use it at your own risk.

This library is currently under active development. The following components are implemented:
* **Authentication:** Full OAuth 2.0 flow for generating and refreshing tokens.
* **Market Data API:** All endpoints are implemented with strongly-typed responses.
* **Real-Time Streamer:** Level 1 Equity and Option quotes are supported.

The **Trading API** (placing, modifying, and canceling orders) is not yet implemented.

---

## 🚀 Getting Started

Add the library to your `Cargo.toml`:
```toml
[dependencies]
schwab_api_rs = "0.2.0"
tokio = { version = "1", features = ["full"] }
anyhow = "1.0"
dotenv = "0.15"
```

## Setup & Authentication

Before you can use the API, you need your App Key (Client ID) and App Secret from your Schwab Developer Portal application.

1. Set Environment Variables: Create a .env file in your project's root directory:

```
SCHWAB_APP_KEY="YOUR_APP_KEY_HERE"
SCHWAB_APP_SECRET="YOUR_APP_SECRET_HERE"
```

2. Run the One-Time Authorization: The first time you use the library, you need to authorize it with your Schwab account. Create a new binary file (e.g., src/bin/auth.rs) and run it.

```
// src/bin/auth.rs
use schwab_api_rs::SchwabAuth;
use dotenv::dotenv;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok(); // Load .env file

    let app_key = env::var("SCHWAB_APP_KEY")?;
    let app_secret = env::var("SCHWAB_APP_SECRET")?;

    let auth = SchwabAuth::default();
    auth.authorize(&app_key, &app_secret).await?;

    println!("Successfully created tokens.json!");

    Ok(())
}
```

Run this with cargo run --bin auth. It will print a URL. Paste it into your browser, log in, grant access, and then paste the final URL from your browser's address bar back into the terminal. This will create a tokens.json file that the library will use from now on.


--------------------------------
## Usage Examples

### REST API Client Example

Here's how to create a client and fetch quotes for a few stocks. The client will automatically refresh your token if it has expired.
```
use schwab_api_rs::SchwabApi;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok(); // Load .env file

    // Create a new API client using credentials from environment variables
    let api = SchwabApi::default().await?;

    let symbols = vec!["AAPL".to_string(), "TSLA".to_string()];

    println!("Fetching quotes for {:?}", symbols);
    let quotes = api.get_quotes(symbols, None, None).await?;

    for (symbol, quote_data) in quotes {
        if let Some(quote) = quote_data.quote {
            println!(
                "  - {}: Last Price: ${:.2}, Volume: {}",
                symbol, quote.last_price, quote.total_volume
            );
        }
    }

    Ok(())
}
```

### WebSocket Streamer Example

The streamer provides real-time data. You start() it to get a channel receiver, then send() subscription requests.
```
use schwab_api_rs::{
    schwab_streamer::{Command, SchwabStreamer},
    models::streamer::StreamerMessage,
};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok(); // Load .env file

    // Create the streamer. It uses the same SchwabApi client internally.
    let streamer = SchwabStreamer::default().await?;
    
    // Start the listener task and get the receiver for messages
    let mut receiver = streamer.start().await?;
    
    // Wait for the streamer to log in and become active
    while !streamer.is_active().await {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    println!("Streamer is active. Subscribing to SPY quotes...");

    // 1. Build a typed subscription request
    let equity_request = streamer.level_one_equities(
        vec!["SPY".to_string()], 
        vec![], // Empty vec subscribes to all available fields
        Command::Subs,
    );

    // 2. Send the request
    streamer.send(vec![equity_request]).await?;

    println!("Subscription sent. Waiting for messages...");

    // 3. Listen for incoming messages on the channel
    while let Some(message) = receiver.recv().await {
        match message {
            StreamerMessage::LevelOneEquity(equity_quote) => {
                println!(
                    "EQUITY -> {}: Bid: {:?}, Ask: {:?}",
                    equity_quote.symbol,
                    equity_quote.bid_price,
                    equity_quote.ask_price
                );
            }
            StreamerMessage::LevelOneOption(option_quote) => {
                println!(
                    "OPTION -> {}: Mark Price: {:?}",
                    option_quote.symbol,
                    option_quote.mark_price
                );
            }
        }
    }

    Ok(())
}
```

## Roadmap

* [ ] Implement a custom, specific Error type.

* [ ] Implement Trading API endpoints (orders, accounts).

* [ ] Add automatic resubscription on streamer reconnect.

* [ ] Add more examples and documentation.


## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## License

This project is licensed under either of:

    Apache License, Version 2.0, (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)

    MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.