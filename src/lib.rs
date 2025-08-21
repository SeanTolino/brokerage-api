//! A Rust library for interacting with the Schwab API.
//!
//! This crate provides a convenient way to authenticate with the Schwab API and access various market data endpoints.
//!
//! ## Modules
//!
//! - `schwab`: Contains modules for Schwab API authentication and market data access.
//! - `util`: Provides utility functions used across the library.

mod util;

/// Provides modules for interacting with the Schwab API, including authentication and market data.
pub mod schwab;

pub use schwab::schwab_api::SchwabApi;
pub use schwab::schwab_auth::SchwabAuth;
pub use schwab::schwab_streamer_api::SchwabStreamerApi;
