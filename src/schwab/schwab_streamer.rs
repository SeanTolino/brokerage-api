use std::{
    collections::HashMap,
    default, fmt, fs,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Instant,
};

use anyhow::anyhow;
use chrono::Utc;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde_json::{Error, Value, json};
use tokio::{net::TcpStream, sync::Mutex, task::JoinHandle};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    SchwabApi,
    schwab::{
        common::{SCHWAB_STREAMER_API_URL, TOKENS_FILE},
        model::streamer::{self, LevelOneOptionsField},
        schwab_auth::StoredTokenInfo,
    },
};

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

#[derive(Eq, PartialEq, Hash)]
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
    async fn connect(
        &mut self,
        streamer_info: Arc<Value>,
        request_id: Arc<AtomicI64>,
    ) -> anyhow::Result<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>, anyhow::Error>
    {
        let schwab_client_channel = match streamer_info.get("schwabClientChannel") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let schwab_client_function_id = match streamer_info.get("schwabClientFunctionId") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let json_string = fs::read_to_string(TOKENS_FILE)?;
        let data: StoredTokenInfo = serde_json::from_str(&json_string)?;
        let auth_header = format!("{}", data.access_token.as_str());

        let (ws_stream, _response) = connect_async(SCHWAB_STREAMER_API_URL)
            .await
            .expect("Failed to connect to stream API");

        let (mut write, read) = ws_stream.split();
        let parameters = json!({
            "Authorization": auth_header,
            "SchwabClientChannel": schwab_client_channel,
            "SchwabClientFunctionId": schwab_client_function_id,
        });
        let message = build_message(request_id, streamer_info, Service::Admin, Command::Login, parameters)?;
        println!("[{:?}] Sending LOGIN request", Utc::now());
        println!("{:?}", message);
        write
            .send(Message::Text(message.to_string().into()))
            .await?;

        self.writer = Some(write);

        Ok(read)
    }

    fn handle_command_response(&mut self, value: &Value) {
        let command: Command = value
            .get("command")
            .map(Value::to_string)
            .map(|cmd| Command::from(cmd))
            .unwrap_or_default();

        match command {
            Command::Add => {
                println!("Received add command response");
            }
            Command::Subs => {
                println!("Received subs command response");
            }
            Command::Unsubs => {
                println!("Received unsubs command response");
            }
            Command::View => {
                println!("View command not supported");
            }
            Command::Login => {
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
                println!("Received logout command response");
            }
            Command::Unknown => {
                println!("Received unknown command response");
            }
        }
    }
}

impl SchwabStreamer {
    pub async fn new(schwab_api: SchwabApi) -> anyhow::Result<Self, anyhow::Error> {
        let user_preferences = schwab_api.get_preferences().await?;
        let streamer_info = user_preferences
            .get("streamerInfo")
            .and_then(|info| info.get(0))
            .ok_or_else(|| anyhow!("Unable to read streamer info"))?;

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
            streamer_info: Arc::new(streamer_info.clone()),
        })
    }

    pub async fn default() -> anyhow::Result<Self, anyhow::Error> {
        let schwab_api = SchwabApi::default();
        SchwabStreamer::new(schwab_api).await
    }

    pub async fn start(&self) -> anyhow::Result<(), anyhow::Error> {
        let inner_clone = self.inner.clone();

        let mut read = {
            let mut guard = self.inner.lock().await;
            guard.connect(self.streamer_info.clone(), self.request_id.clone()).await?
        };

        let listener = tokio::spawn(async move {
            while let Some(message_result) = read.next().await {
                if let Ok(msg) = message_result {
                    if let Ok(text) = msg.into_text() {
                        if let Ok(json_data) = serde_json::from_str::<Value>(&text) {
                            if let Some(response_array) =
                                json_data.get("response").and_then(Value::as_array)
                            {
                                // Lock the mutex once per message to handle all responses inside it.
                                let mut guard = inner_clone.lock().await;
                                for r in response_array {
                                    guard.handle_command_response(r);
                                }
                            }
                            if let Some(data_array) =
                                json_data.get("data").and_then(Value::as_array)
                            {
                                for d in data_array {
                                    if let Some(service) = d.get("service") {
                                        match Service::from(service.to_string()) {
                                            Service::LevelOneOptions => {
                                                println!("LEVEL ONE OPTIONS DATA: {:?}", d);
                                            }
                                            Service::LevelOneEquities => {
                                                println!("LEVEL ONE EQUITIES DATA: {:?}", d);
                                            }
                                            Service::Admin => {
                                                println!("Admin service data received");
                                            }
                                            _ => {
                                                println!("Unknown service data received");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Store the handle to the spawned task.
        self.inner.lock().await.listener_handle = Some(Arc::new(listener));

        Ok(())
    }

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
    ) -> anyhow::Result<(), anyhow::Error> {
        let mut guard = self.inner.lock().await;
        if let Some(writer) = guard.writer.as_mut() {
            for request in stream_requests {
                println!("Sending request: {:?}", request);
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
    ) -> anyhow::Result<Value, anyhow::Error> {
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
    ) -> anyhow::Result<Value, anyhow::Error> {
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

    pub fn is_active(&self) -> bool {
        // This is efficient because we can check the Arc<AtomicBool>
        // without locking the main mutex.
        let inner = self.inner.blocking_lock();
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
) -> anyhow::Result<Value, anyhow::Error> {
    let request_id = request_id.fetch_add(1, Ordering::Relaxed);
    let schwab_client_customer_id = match streamer_info.get("schwabClientCustomerId") {
        Some(i) => i,
        None => {
            return Err(anyhow!("Unable to read streamer info"));
        }
    };
    let schwab_client_correlation_id = match streamer_info.get("schwabClientCorrelId") {
        Some(i) => i,
        None => {
            return Err(anyhow!("Unable to read streamer info"));
        }
    };
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
