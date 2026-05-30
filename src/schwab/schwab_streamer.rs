use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use anyhow::anyhow;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn, trace};

use crate::{
    schwab::{
        common::SCHWAB_STREAMER_API_URL,
        models::{
            streamer::{
                self, LevelOneEquitiesResponse, LevelOneForexField, LevelOneForexResponse, LevelOneFuturesField, LevelOneFuturesOptionsField, LevelOneFuturesOptionsResponse, LevelOneFuturesResponse, LevelOneOptionsField, LevelOneOptionsResponse, StreamerMessage
            },
            trader::UserPreferencesResponse,
        },
    },
    SchwabApi,
};

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub enum Command {
    Add,
    Subs,
    Unsubs,
    View,
    Login,
    Logout,
    #[default]
    Unknown,
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

impl From<&str> for Command {
    fn from(s: &str) -> Command {
        match s {
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

impl<'de> serde::Deserialize<'de> for Command {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Command::from(s.as_str()))
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum Service {
    LevelOneOptions,
    LevelOneEquities,
    LevelOneFutures,
    LevelOneFuturesOptions,
    LevelOneForex,
    Admin,
    Unknown,
}

impl From<&str> for Service {
    fn from(s: &str) -> Service {
        match s {
            "LEVELONE_EQUITIES" => Service::LevelOneEquities,
            "LEVELONE_OPTIONS" => Service::LevelOneOptions,
            "LEVELONE_FUTURES" => Service::LevelOneFutures,
            "LEVELONE_FUTURES_OPTIONS" => Service::LevelOneFuturesOptions,
            "LEVELONE_FOREX" => Service::LevelOneForex,
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
            Service::LevelOneFuturesOptions => write!(f, "LEVELONE_FUTURES_OPTIONS"),
            Service::LevelOneForex => write!(f, "LEVELONE_FOREX"),
            Service::LevelOneFutures => write!(f, "LEVELONE_FUTURES"),
            Service::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Service {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Service::from(s.as_str()))
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "service", content = "content")]
enum StreamerData {
    #[serde(rename = "LEVELONE_EQUITIES")]
    LevelOneEquities(Vec<LevelOneEquitiesResponse>),
    #[serde(rename = "LEVELONE_OPTIONS")]
    LevelOneOptions(Vec<LevelOneOptionsResponse>),
    #[serde(rename = "LEVELONE_FUTURES")]
    LevelOneFutures(Vec<LevelOneFuturesResponse>),
    #[serde(rename = "LEVELONE_FUTURES_OPTIONS")]
    LevelOneFuturesOptions(Vec<LevelOneFuturesOptionsResponse>),
    #[serde(rename = "LEVELONE_FOREX")]
    LevelOneForex(Vec<LevelOneForexResponse>),
    #[serde(rename = "ADMIN")]
    Admin(()),
}

impl From<StreamerData> for Vec<StreamerMessage> {
    fn from(streamer_data: StreamerData) -> Self {
        match streamer_data {
            StreamerData::LevelOneEquities(content) => content
                .into_iter()
                .map(StreamerMessage::LevelOneEquity)
                .collect(),
            StreamerData::LevelOneOptions(content) => content
                .into_iter()
                .map(StreamerMessage::LevelOneOption)
                .collect(),
            StreamerData::LevelOneFutures(content) => content
                .into_iter()
                .map(StreamerMessage::LevelOneFutures)
                .collect(),
            StreamerData::LevelOneFuturesOptions(content) => content
                .into_iter()
                .map(StreamerMessage::LevelOneFuturesOptions)
                .collect(),
            StreamerData::LevelOneForex(content) => content
                .into_iter()
                .map(StreamerMessage::LevelOneForex)
                .collect(),
            StreamerData::Admin(()) => {
                tracing::warn!("Received unhandled admin message");
                vec![]
            }
        }
    }
}

#[derive(Deserialize, Debug)]
struct StreamerResponse {
    command: Command,
    content: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct TopLevelMessage {
    #[serde(default)]
    response: Vec<StreamerResponse>,
    #[serde(default)]
    data: Vec<StreamerData>,
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

    fn handle_command_response(&mut self, response: &StreamerResponse) {
        match response.command {
            Command::Add | Command::Subs | Command::Unsubs => {
                debug!("Received subscription response: {:?}", response);
            }
            Command::View => {
                debug!("View command not supported");
            }
            Command::Login => {
                debug!("Received login response: {:?}", response);
                if let Some(content) = &response.content {
                    if let Some(code) = content.get("code").and_then(Value::as_u64) {
                        if code == 0 {
                            self.is_active.store(true, Ordering::SeqCst);
                        }
                    }
                }
            }
            Command::Logout => {
                debug!("Received logout response: {:?}", response);
            }
            Command::Unknown => {
                debug!("Received unknown command response: {:?}", response);
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

#[derive(Debug, Clone, Default)]
pub struct SchwabStreamerStartConfig {
    pub connect_timeout: Option<Duration>,
    pub login_send_timeout: Option<Duration>,
    pub login_ack_timeout: Option<Duration>,
}

impl SchwabStreamer {
    pub async fn new(schwab_api: SchwabApi) -> anyhow::Result<Self> {
        let user_preferences: UserPreferencesResponse = schwab_api.get_preferences().await?;

        let streamer_info = user_preferences
            .streamer_info
            .first()
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
        self.start_with_config(SchwabStreamerStartConfig::default())
            .await
    }

    pub async fn start_with_config(
        &self,
        config: SchwabStreamerStartConfig,
    ) -> anyhow::Result<mpsc::Receiver<StreamerMessage>> {
        let inner_clone = self.inner.clone();

        let (tx, rx) = mpsc::channel(100);

        let mut read = {
            let mut guard = self.inner.lock().await;

            let token_info = guard.schwab_api.token_info().await;
            let auth_header = token_info.access_token.as_str();

            let (ws_stream, _response) = if let Some(connect_timeout) = config.connect_timeout {
                timeout(connect_timeout, connect_async(SCHWAB_STREAMER_API_URL))
                    .await
                    .map_err(|_| anyhow!("Timed out connecting to stream API"))?
                    .expect("Failed to connect to stream API")
            } else {
                connect_async(SCHWAB_STREAMER_API_URL)
                    .await
                    .expect("Failed to connect to stream API")
            };

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
            if let Some(login_send_timeout) = config.login_send_timeout {
                timeout(
                    login_send_timeout,
                    write.send(Message::Text(message.to_string().into())),
                )
                .await
                .map_err(|_| anyhow!("Timed out sending LOGIN request"))??;
            } else {
                write
                    .send(Message::Text(message.to_string().into()))
                    .await?;
            }

            guard.writer = Some(write);
            read
        };

        let wait_for_login_ack = async {
            loop {
                let message_result = read
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("WebSocket stream ended before LOGIN response"))?;

                match message_result {
                    Ok(msg) => {
                        if let Ok(text) = msg.into_text() {
                            match serde_json::from_str::<TopLevelMessage>(&text) {
                                Ok(message) => {
                                    let mut saw_login_response = false;
                                    if !message.response.is_empty() {
                                        let mut guard = inner_clone.lock().await;
                                        for r in &message.response {
                                            if r.command == Command::Login {
                                                saw_login_response = true;
                                            }
                                            guard.handle_command_response(r);
                                        }
                                    }

                                    if !message.data.is_empty() {
                                        for streamer_data in message.data {
                                            let messages: Vec<StreamerMessage> = streamer_data.into();
                                            for msg in messages {
                                                if tx.send(msg).await.is_err() {
                                                    return Err(anyhow!(
                                                        "Stream receiver dropped before LOGIN ack"
                                                    ));
                                                }
                                            }
                                        }
                                    }

                                    if saw_login_response {
                                        break Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize message: {}, error: {}", text, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(anyhow!("Error reading from WebSocket stream: {}", e));
                    }
                }
            }
        };

        if let Some(login_ack_timeout) = config.login_ack_timeout {
            timeout(login_ack_timeout, wait_for_login_ack)
                .await
                .map_err(|_| anyhow!("Timed out waiting for LOGIN response"))??;
        } else {
            wait_for_login_ack.await?;
        }

        let listener = tokio::spawn(async move {
            while let Some(message_result) = read.next().await {
                trace!("READER RECEIVED: {:?}", message_result);
                match message_result {
                    Ok(msg) => {
                        if let Ok(text) = msg.into_text() {
                            match serde_json::from_str::<TopLevelMessage>(&text) {
                                Ok(message) => {
                                    if !message.response.is_empty() {
                                        let mut guard = inner_clone.lock().await;
                                        for r in &message.response {
                                            guard.handle_command_response(r);
                                        }
                                    }

                                    if !message.data.is_empty() {
                                        for streamer_data in message.data {
                                            let messages: Vec<StreamerMessage> = streamer_data.into();

                                            for msg in messages {
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
                                Err(e) => {
                                    warn!("Failed to deserialize message: {}, error: {}", text, e);
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

    pub fn level_one_futures(
        &self,
        keys: Vec<String>,
        fields: Vec<LevelOneFuturesField>,
        command: Command,
    ) -> StreamRequest {
        let fields_as_strings: Vec<String> = if fields.is_empty() {
            (0..=40).map(|v| v.to_string()).collect()
        } else {
            fields.iter().map(|f| f.to_string()).collect()
        };

        StreamRequest::new(Service::LevelOneFutures, command, keys, fields_as_strings)
    }

    pub fn level_one_futures_options(
        &self,
        keys: Vec<String>,
        fields: Vec<LevelOneFuturesOptionsField>,
        command: Command,
    ) -> StreamRequest {
        let fields_as_strings: Vec<String> = if fields.is_empty() {
            (0..=31).map(|v| v.to_string()).collect()
        } else {
            fields.iter().map(|f| f.to_string()).collect()
        };

        StreamRequest::new(Service::LevelOneFuturesOptions, command, keys, fields_as_strings)
    }

    pub fn level_one_forex(
        &self,
        keys: Vec<String>,
        fields: Vec<LevelOneForexField>,
        command: Command,
    ) -> StreamRequest {
        let fields_as_strings: Vec<String> = if fields.is_empty() {
            (0..=29).map(|v| v.to_string()).collect()
        } else {
            fields.iter().map(|f| f.to_string()).collect()
        };

        StreamRequest::new(Service::LevelOneForex, command, keys, fields_as_strings)
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
