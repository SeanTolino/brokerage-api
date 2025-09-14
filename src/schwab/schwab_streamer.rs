// src/schwab/schwab_streamer.rs

use std::{
    collections::HashMap,
    default, env, fmt, fs,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::anyhow;
use chrono::Utc;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde_json::{json, Value};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex}, // <-- Added mpsc
    task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

use crate::{
    schwab::{
        common::{SCHWAB_STREAMER_API_URL, TOKENS_FILE},
        models::{
            streamer::{
                self, LevelOneEquitiesResponse, LevelOneOptionsField, LevelOneOptionsResponse,
                StreamerMessage, // <-- IMPORT THE NEW ENUM
            },
            trader::UserPreferencesResponse,
        },
        schwab_auth::StoredTokenInfo,
    },
    SchwabApi,
};

// ... (enums for Command and Service are unchanged) ...
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Add,
    Subs,
    Unsubs,
    View,
    Login,
    Logout,
    Unknown,
}

impl Default for Command {
    fn default() -> Self {
        Command::Unknown
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Add => write!(f, "ADD"),
            Command::Subs => write!(f, "SUBS"),
            Command::Unsubs => write!(f, "UNSUBS"),
            Command::View => write!(f, "VIEW"),
            Command::Login => write!(f, "LOGIN"),
            Command::Logout => write!(f, "LOGOUT"),
            Command::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl From<String> for Command {
    fn from(s: String) -> Command {
        match s.as_str() {
            "ADD" => Command::Add,
            "SUBS" => Command::Subs,
            "UNSUBS" => Command::Unsubs,
            "VIEW" => Command::View,
            "LOGIN" => Command::Login,
            "LOGOUT" => Command::Logout,
            _ => Command::Unknown,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum Service {
    LevelOneOptions,
    LevelOneEquities,
    Admin,
    Unknown,
}

impl From<String> for Service {
    fn from(s: String) -> Service {
        match s.as_str() {
            "LEVELONE_EQUITIES" => Service::LevelOneEquities,
            "LEVELONE_OPTIONS" => Service::LevelOneOptions,
            "ADMIN" => Service::Admin,
            _ => Service::Unknown,
        }
    }
}

impl fmt::Display for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Service::Admin => write!(f, "ADMIN"),
            Service::LevelOneOptions => write!(f, "LEVELONE_OPTIONS"),
            Service::LevelOneEquities => write!(f, "LEVELONE_EQUITIES"),
            Service::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

pub struct StreamRequest {
    service: Service,
    command: Command,
    keys: Vec<String>,
    fields: Vec<String>,
}

impl StreamRequest {
    pub fn new(service: Service, command: Command, keys: Vec<String>, fields: Vec<String>) -> Self {
        Self {
            service,
            command,
            keys,
            fields,
        }
    }
}

#[derive(Debug)]
struct SchwabStreamerInner {
    schwab_api: SchwabApi,
    subscriptions: HashMap<Service, HashMap<String, Vec<String>>>,
    writer: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    listener_handle: Option<Arc<JoinHandle<()>>>,
    is_active: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct SchwabStreamer {
    inner: Arc<Mutex<SchwabStreamerInner>>,
    request_id: Arc<AtomicI64>,
    streamer_info: Arc<Value>,
}

impl SchwabStreamerInner {
    #[tracing::instrument]
    async fn connect(
        &mut self,
        streamer_info: Arc<Value>,
        request_id: Arc<AtomicI64>,
    ) -> anyhow::Result<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>, anyhow::Error>
    {
        let schwab_client_channel = streamer_info
            .get("schwabClientChannel")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Unable to read schwabClientChannel from streamer info"))?;
        let schwab_client_function_id = streamer_info
            .get("schwabClientFunctionId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Unable to read schwabClientFunctionId from streamer info"))?;

        let json_string = fs::read_to_string(TOKENS_FILE)?;
        let data: StoredTokenInfo = serde_json::from_str(&json_string)?;
        let auth_header = data.access_token.as_str();

        let (ws_stream, _response) = connect_async(SCHWAB_STREAMER_API_URL)
            .await
            .expect("Failed to connect to stream API");

        let (mut write, read) = ws_stream.split();
        let parameters = json!({
            "Authorization": auth_header,
            "SchwabClientChannel": schwab_client_channel,
            "SchwabClientFunctionId": schwab_client_function_id,
        });
        let message = build_message(
            request_id,
            streamer_info,
            Service::Admin,
            Command::Login,
            parameters,
        )?;
        info!("[{:?}] Sending LOGIN request", Utc::now());
        info!("{:?}", message);
        write
            .send(Message::Text(message.to_string().into()))
            .await?;

        self.writer = Some(write);

        Ok(read)
    }

    fn handle_command_response(&mut self, value: &Value) {
        let command: Command = value
            .get("command")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .map(Command::from)
            .unwrap_or_default();

        match command {
            Command::Add => {
                info!("Received add command response: {:?}", value);
            }
            Command::Subs => {
                info!("Received subs command response: {:?}", value);
            }
            Command::Unsubs => {
                info!("Received unsubs command response: {:?}", value);
            }
            Command::View => {
                info!("View command not supported");
            }
            Command::Login => {
                info!("Received login command response: {:?}", value);
                if let Some(code) = value
                    .get("content")
                    .and_then(|content| content.get("code"))
                    .and_then(Value::as_u64)
                {
                    if code == 0 {
                        self.is_active
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
            Command::Logout => {
                info!("Received logout command response: {:?}", value);
            }
            Command::Unknown => {
                info!("Received unknown command response: {:?}", value);
            }
        }
    }
}

impl SchwabStreamer {
    pub async fn new(schwab_api: SchwabApi) -> anyhow::Result<Self> {
        let user_preferences: UserPreferencesResponse = schwab_api.get_preferences().await?;

        let streamer_info = user_preferences
            .streamer_info
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Streamer info not found in user preferences"))?;

        let streamer_info_value = serde_json::to_value(streamer_info)?;

        let inner_state = SchwabStreamerInner {
            schwab_api,
            subscriptions: HashMap::new(),
            writer: None,
            listener_handle: None,
            is_active: Arc::new(AtomicBool::new(false)),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner_state)),
            request_id: Arc::new(AtomicI64::new(0)),
            streamer_info: Arc::new(streamer_info_value),
        })
    }

    pub async fn default() -> anyhow::Result<Self> {
        let schwab_api = SchwabApi::default().await?;
        SchwabStreamer::new(schwab_api).await
    }

    /// Starts the WebSocket listener and returns a channel receiver to get incoming data.
    ///
    /// # Returns
    ///
    /// A `tokio::sync::mpsc::Receiver` that will receive `StreamerMessage` enums.
    pub async fn start(&self) -> anyhow::Result<mpsc::Receiver<StreamerMessage>> {
        let inner_clone = self.inner.clone();
        
        // Create a channel for sending data out to the user.
        let (tx, rx) = mpsc::channel(100);

        let mut read = {
            let mut guard = self.inner.lock().await;
            guard
                .connect(self.streamer_info.clone(), self.request_id.clone())
                .await?
        };

        let listener = tokio::spawn(async move {
            while let Some(message_result) = read.next().await {
                match message_result {
                    Ok(msg) => {
                        if let Ok(text) = msg.into_text() {
                            if let Ok(json_data) = serde_json::from_str::<Value>(&text) {
                                // Handle command responses
                                if let Some(responses) = json_data.get("response").and_then(Value::as_array) {
                                    let mut guard = inner_clone.lock().await;
                                    for r in responses {
                                        guard.handle_command_response(r);
                                    }
                                }
                                
                                // Handle data payloads and send them to the channel
                                if let Some(data_array) = json_data.get("data").and_then(Value::as_array) {
                                    for d in data_array {
                                        let service_str = d.get("service").and_then(Value::as_str).unwrap_or("");
                                        let service = Service::from(service_str.to_string());
                                        
                                        if let Some(content) = d.get("content").and_then(Value::as_array) {
                                            for item in content {
                                                let message = match service {
                                                    Service::LevelOneEquities => {
                                                        match serde_json::from_value::<LevelOneEquitiesResponse>(item.clone()) {
                                                            Ok(equity_data) => Some(StreamerMessage::LevelOneEquity(equity_data)),
                                                            Err(e) => {
                                                                warn!("Failed to deserialize LevelOneEquitiesResponse: {}", e);
                                                                None
                                                            }
                                                        }
                                                    },
                                                    Service::LevelOneOptions => {
                                                        match serde_json::from_value::<LevelOneOptionsResponse>(item.clone()) {
                                                            Ok(option_data) => Some(StreamerMessage::LevelOneOption(option_data)),
                                                            Err(e) => {
                                                                warn!("Failed to deserialize LevelOneOptionsResponse: {}", e);
                                                                None
                                                            }
                                                        }
                                                    },
                                                    _ => None,
                                                };

                                                if let Some(msg) = message {
                                                    if tx.send(msg).await.is_err() {
                                                        info!("Stream receiver dropped. Closing listener task.");
                                                        return; // Exit task if the receiver is gone
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        warn!("Error reading from WebSocket stream: {}", e);
                        break;
                    }
                }
            }
        });

        self.inner.lock().await.listener_handle = Some(Arc::new(listener));

        Ok(rx)
    }
    
    // ... rest of the file is the same
    pub async fn get_listener(&self) -> Option<Arc<JoinHandle<()>>> {
        let guard = self.inner.lock().await;
        guard.listener_handle.as_ref().map(Arc::clone)
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(writer) = guard.writer.as_mut() {
            writer.close().await?;
        }
        if let Some(handle) = guard.listener_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    pub async fn send_requests(
        &self,
        stream_requests: Vec<Value>,
    ) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(writer) = guard.writer.as_mut() {
            for request in stream_requests {
                info!("Sending request: {:?}", request);
                writer
                    .send(Message::Text(request.to_string().into()))
                    .await?;
            }
        }
        Ok(())
    }

    pub fn level_one_equities(
        &mut self,
        keys: Vec<String>,
        fields: Vec<streamer::LevelOneEquitiesField>,
        command: Command,
    ) -> anyhow::Result<Value> {
        let fields: String = if fields.len() > 0 {
            fields
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(",")
        } else {
            (0..=51)
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(",")
        };
        let parameters = json!({
            "keys": keys.join(","),
            "fields": fields,
        });
        Ok(build_message(
            self.request_id.clone(),
            self.streamer_info.clone(),
            Service::LevelOneEquities,
            command,
            parameters,
        )?)
    }

    pub fn level_one_options(
        &mut self,
        keys: Vec<String>,
        fields: Vec<LevelOneOptionsField>,
        command: Command,
    ) -> anyhow::Result<Value> {
        let fields: String = if fields.len() > 0 {
            fields
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(",")
        } else {
            (0..=55)
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(",")
        };
        let parameters = json!({
            "keys": keys.join(","),
            "fields": fields,
        });
        Ok(build_message(
            self.request_id.clone(),
            self.streamer_info.clone(),
            Service::LevelOneOptions,
            command,
            parameters,
        )?)
    }

    pub async fn is_active(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.is_active.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn record_request(&mut self, stream_request: StreamRequest) {
        let mut guard = self.inner.lock().await;
        let service = stream_request.service;
        let command = stream_request.command;
        let keys = stream_request.keys;
        let fields = stream_request.fields;

        match command {
            Command::Add => {
                let service = guard.subscriptions.get_mut(&service);
                if let Some(s) = service {
                    for key in keys {
                        match s.get_mut(&key) {
                            Some(v) => {
                                v.extend(fields.clone());
                            }
                            None => {
                                s.insert(key, fields.clone());
                            }
                        }
                    }
                }
            }
            Command::Subs => {
                let service = guard.subscriptions.get_mut(&service);
                if let Some(s) = service {
                    for key in keys {
                        s.insert(key, fields.clone());
                    }
                }
            }
            Command::Unsubs => {
                let service = guard.subscriptions.get_mut(&service);
                if let Some(s) = service {
                    for key in keys {
                        s.remove(&key);
                    }
                }
            }
            Command::View => {
                panic!("VIEW command not implemented");
            }
            _ => {
                panic!("{}", format!("{} is an unsupported command", command));
            }
        }
    }
}

fn build_message(
    request_id: Arc<AtomicI64>,
    streamer_info: Arc<Value>,
    service: Service,
    command: Command,
    parameters: Value,
) -> anyhow::Result<Value> {
    let request_id = request_id.fetch_add(1, Ordering::Relaxed);
    let schwab_client_customer_id = streamer_info
        .get("schwabClientCustomerId")
        .ok_or_else(|| anyhow!("Unable to read streamer info"))?;
    let schwab_client_correlation_id = streamer_info
        .get("schwabClientCorrelId")
        .ok_or_else(|| anyhow!("Unable to read streamer info"))?;
    Ok(json!({
        "service": service.to_string(),
        "command": command.to_string(),
        "requestid": request_id + 1,
        "parameters": parameters,
        "SchwabClientCustomerId": schwab_client_customer_id,
        "SchwabClientCorrelId": schwab_client_correlation_id,
    }))
}

#[cfg(test)]
mod test {}