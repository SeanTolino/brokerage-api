use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};

use anyhow::anyhow;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde_json::{Value, json};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn};

use crate::{
    SchwabApi,
    schwab::{
        common::SCHWAB_STREAMER_API_URL,
        models::{
            streamer::{
                self, LevelOneEquitiesResponse, LevelOneOptionsField, LevelOneOptionsResponse,
                StreamerMessage,
            },
            trader::UserPreferencesResponse,
        },
    },
};

#[derive(Debug, PartialEq, Eq, Clone)]
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

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
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

#[derive(Debug, Clone)]
pub struct StreamRequest {
    pub service: Service,
    pub command: Command,
    pub keys: Vec<String>,
    pub fields: Vec<String>,
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

impl SchwabStreamerInner {
    fn record_request(&mut self, stream_request: &StreamRequest) {
        let service_map = self
            .subscriptions
            .entry(stream_request.service.clone())
            .or_default();

        match stream_request.command {
            Command::Add => {
                for key in &stream_request.keys {
                    let key_fields = service_map.entry(key.clone()).or_default();
                    key_fields.extend(stream_request.fields.clone());
                    key_fields.sort();
                    key_fields.dedup();
                }
            }
            Command::Subs => {
                for key in &stream_request.keys {
                    service_map.insert(key.clone(), stream_request.fields.clone());
                }
            }
            Command::Unsubs => {
                for key in &stream_request.keys {
                    service_map.remove(key);
                }
            }
            _ => {}
        }
    }

    fn handle_command_response(&mut self, value: &Value) {
        let command: Command = value
            .get("command")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .map(Command::from)
            .unwrap_or_default();

        match command {
            Command::Add | Command::Subs | Command::Unsubs => {
                debug!("Received subscription response: {:?}", value);
            }
            Command::View => {
                debug!("View command not supported");
            }
            Command::Login => {
                debug!("Received login response: {:?}", value);
                if let Some(code) = value
                    .get("content")
                    .and_then(|content| content.get("code").and_then(Value::as_u64))
                {
                    if code == 0 {
                        self.is_active.store(true, Ordering::SeqCst);
                    }
                }
            }
            Command::Logout => {
                debug!("Received logout response: {:?}", value);
            }
            Command::Unknown => {
                debug!("Received unknown command response: {:?}", value);
            }
        }
    }
}

#[derive(Clone)]
pub struct SchwabStreamer {
    inner: Arc<Mutex<SchwabStreamerInner>>,
    request_id: Arc<AtomicI64>,
    streamer_info: Arc<Value>,
}

impl SchwabStreamer {
    pub async fn new(schwab_api: SchwabApi) -> anyhow::Result<Self> {
        let user_preferences: UserPreferencesResponse = schwab_api.get_preferences().await?;

        let streamer_info = user_preferences
            .streamer_info
            .get(0)
            .ok_or_else(|| anyhow!("Streamer info not found in user preferences"))?;

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

    pub async fn start(&self) -> anyhow::Result<mpsc::Receiver<StreamerMessage>> {
        let inner_clone = self.inner.clone();

        let (tx, rx) = mpsc::channel(100);

        let mut read = {
            let mut guard = self.inner.lock().await;

            let token_info = guard.schwab_api.token_info().await;
            let auth_header = token_info.access_token.as_str();

            let (ws_stream, _response) = connect_async(SCHWAB_STREAMER_API_URL)
                .await
                .expect("Failed to connect to stream API");

            let (mut write, read) = ws_stream.split();

            let parameters = json!({
                "qoslevel": "0",
                "Authorization": auth_header,
                "SchwabClientChannel": self.streamer_info.get("schwabClientChannel"),
                "SchwabClientFunctionId": self.streamer_info.get("schwabClientFunctionId"),
            });

            let message = build_message(
                self.request_id.clone(),
                self.streamer_info.clone(),
                Service::Admin,
                Command::Login,
                parameters,
            )?;

            debug!("[{:?}] Sending LOGIN request", Utc::now());
            write
                .send(Message::Text(message.to_string().into()))
                .await?;

            guard.writer = Some(write);
            read
        };

        let listener = tokio::spawn(async move {
            while let Some(message_result) = read.next().await {
                debug!("READER RECEIVED: {:?}", message_result);
                match message_result {
                    Ok(msg) => {
                        if let Ok(text) = msg.into_text() {
                            if let Ok(json_data) = serde_json::from_str::<Value>(&text) {
                                if let Some(responses) =
                                    json_data.get("response").and_then(Value::as_array)
                                {
                                    let mut guard = inner_clone.lock().await;
                                    for r in responses {
                                        guard.handle_command_response(r);
                                        // if let Some(content) = r.get("content") {
                                        //     guard.handle_command_response(content);
                                        // }
                                    }
                                }

                                if let Some(data_array) =
                                    json_data.get("data").and_then(Value::as_array)
                                {
                                    for d in data_array {
                                        let service_str =
                                            d.get("service").and_then(Value::as_str).unwrap_or("");
                                        let service = Service::from(service_str.to_string());

                                        if let Some(content) =
                                            d.get("content").and_then(Value::as_array)
                                        {
                                            for item in content {
                                                let message = match service {
                                                    Service::LevelOneEquities => {
                                                        match serde_json::from_value::<
                                                            LevelOneEquitiesResponse,
                                                        >(
                                                            item.clone()
                                                        ) {
                                                            Ok(equity_data) => Some(
                                                                StreamerMessage::LevelOneEquity(
                                                                    equity_data,
                                                                ),
                                                            ),
                                                            Err(e) => {
                                                                warn!(
                                                                    "Failed to deserialize LevelOneEquitiesResponse: {}",
                                                                    e
                                                                );
                                                                None
                                                            }
                                                        }
                                                    }
                                                    Service::LevelOneOptions => {
                                                        match serde_json::from_value::<
                                                            LevelOneOptionsResponse,
                                                        >(
                                                            item.clone()
                                                        ) {
                                                            Ok(option_data) => Some(
                                                                StreamerMessage::LevelOneOption(
                                                                    option_data,
                                                                ),
                                                            ),
                                                            Err(e) => {
                                                                warn!(
                                                                    "Failed to deserialize LevelOneOptionsResponse: {}",
                                                                    e
                                                                );
                                                                None
                                                            }
                                                        }
                                                    }
                                                    _ => None,
                                                };

                                                if let Some(msg) = message {
                                                    if tx.send(msg).await.is_err() {
                                                        debug!(
                                                            "Stream receiver dropped. Closing listener task."
                                                        );
                                                        return;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
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

    pub async fn send(&self, requests: Vec<StreamRequest>) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut writer) = guard.writer.take() {
            for request in requests {
                guard.record_request(&request);

                let parameters = json!({
                    "keys": request.keys.join(","),
                    "fields": request.fields.join(","),
                });

                let message = build_message(
                    self.request_id.clone(),
                    self.streamer_info.clone(),
                    request.service,
                    request.command,
                    parameters,
                )?;

                debug!("Sending request: {:?}", message);
                writer
                    .send(Message::Text(message.to_string().into()))
                    .await?;
            }
            // Put the writer back after the loop
            guard.writer = Some(writer);
        } else {
            return Err(anyhow!("Streamer is not connected. Call start() first."));
        }
        Ok(())
    }

    pub fn level_one_equities(
        &self,
        keys: Vec<String>,
        fields: Vec<streamer::LevelOneEquitiesField>,
        command: Command,
    ) -> StreamRequest {
        let fields_as_strings: Vec<String> = if fields.is_empty() {
            (0..=51).map(|f| f.to_string()).collect()
        } else {
            fields.iter().map(|f| f.to_string()).collect()
        };

        StreamRequest::new(Service::LevelOneEquities, command, keys, fields_as_strings)
    }

    pub fn level_one_options(
        &self,
        keys: Vec<String>,
        fields: Vec<LevelOneOptionsField>,
        command: Command,
    ) -> StreamRequest {
        let fields_as_strings: Vec<String> = if fields.is_empty() {
            (0..=55).map(|v| v.to_string()).collect()
        } else {
            fields.iter().map(|f| f.to_string()).collect()
        };

        StreamRequest::new(Service::LevelOneOptions, command, keys, fields_as_strings)
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

    pub async fn is_active(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.is_active.load(Ordering::SeqCst)
    }
}

fn build_message(
    request_id: Arc<AtomicI64>,
    streamer_info: Arc<Value>,
    service: Service,
    command: Command,
    parameters: Value,
) -> anyhow::Result<Value> {
    let request_id_num = request_id.fetch_add(1, Ordering::Relaxed);
    let schwab_client_customer_id = streamer_info
        .get("schwabClientCustomerId")
        .ok_or_else(|| anyhow!("Unable to read schwabClientCustomerId from streamer info"))?;
    let schwab_client_correl_id = streamer_info
        .get("schwabClientCorrelId")
        .ok_or_else(|| anyhow!("Unable to read schwabClientCorrelId from streamer info"))?;

    let message = json!({
        "requests": [{
            "service": service.to_string(),
            "command": command.to_string(),
            "requestid": request_id_num,
            "parameters": parameters,
            "SchwabClientCustomerId": schwab_client_customer_id,
            "SchwabClientCorrelId": schwab_client_correl_id,
        }]
    });
    Ok(message)
}
