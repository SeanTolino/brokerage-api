//! This module provides functionalities for interacting with the Schwab API.
//!
//! It includes sub-modules for:
//! - `schwab_api`: Core API client for market data and trading operations.
//! - `schwab_auth`: Handles the authentication and token management process.
//! - `common`: Defines common constants and utilities for the Schwab API integration.

pub mod schwab_api;
pub mod schwab_streamer;
pub mod schwab_auth;
pub mod models;
mod common;
