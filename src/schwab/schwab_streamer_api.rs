use std::{collections::HashMap, fmt, fs};

use anyhow::anyhow;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde_json::{Value, json};
use tokio::{io::AsyncWriteExt, net::TcpStream, task::JoinHandle};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    SchwabApi,
    schwab::{
        common::{SCHWAB_STREAMER_API_URL, TOKENS_FILE},
        schwab_auth::StoredTokenInfo,
    },
};

#[derive(Debug)]
pub enum Command {
    Add,
    Subs,
    Unsubs,
    View,
    Login,
    Logout,
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
        }
    }
}

#[derive(Eq, PartialEq, Hash)]
pub enum Service {
    LevelOneOptions,
    LevelOneEquities,
    Admin,
}

impl fmt::Display for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Service::Admin => write!(f, "ADMIN"),
            Service::LevelOneOptions => write!(f, "LEVELONE_OPTIONS"),
            Service::LevelOneEquities => write!(f, "LEVELONE_EQUITIES"),
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
    pub fn new(
        service: Service,
        command: Command,
        keys: Vec<String>,
        fields: Vec<String>,
    ) -> Self {
        Self {
            service,
            command,
            keys,
            fields,
        }
    }
}

pub struct SchwabStreamerApi {
    schwab_api: SchwabApi,
    request_id: i64,
    streamer_info: Value,
    subscriptions: HashMap<Service, HashMap<String, Vec<String>>>,
    writer: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    listener_handle: Option<JoinHandle<()>>,
}

impl SchwabStreamerApi {
    pub async fn new(schwab_api: SchwabApi) -> anyhow::Result<Self, anyhow::Error> {
        let user_preferences = schwab_api.get_preferences().await?;
        let streamer_info = serde_json::from_str::<Value>(&user_preferences.as_str())?;
        let streamer_info = match streamer_info
            .get("streamerInfo")
            .and_then(|info| info.get(0))
        {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        Ok(Self {
            schwab_api,
            request_id: 0,
            streamer_info: streamer_info.clone(),
            subscriptions: HashMap::new(),
            writer: None,
            listener_handle: None,
        })
    }

    pub async fn send_requests(
        &mut self,
        stream_requests: Vec<StreamRequest>,
    ) -> anyhow::Result<(), anyhow::Error> {
        let mut requests = Vec::new();
        for stream_request in stream_requests {
            let parameters = json!({
                "keys": stream_request.keys.join(","),
                "fields": stream_request.fields.join(","),
            });
            let request =
                self.build_message(stream_request.service, stream_request.command, parameters)?;
            requests.push(request);
        }

        if let Some(writer) = self.writer.as_mut() {
            for request in requests {
                writer
                    .send(Message::Text(request.to_string().into()))
                    .await?;
            }
        }

        Ok(())
    }

    pub fn level_one_options(
        &mut self,
        keys: Vec<String>,
        fields: Vec<String>,
        command: Command,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let parameters = json!({
            "parameters": {
                "keys": keys,
                "fields": fields,
            }
        });
        Ok(self.build_message(Service::LevelOneOptions, command, parameters)?)
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let read = self.connect().await?;
        let listener = tokio::spawn(async move {
            read.for_each(|message| async {
                println!("receiving...");
                let data = message.unwrap().into_data();
                tokio::io::stdout().write(&data).await.unwrap();
                println!("received...");
            })
            .await;
        });
        self.listener_handle = Some(listener);
        Ok(())
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.close().await?;
        }
        if let Some(handle) = self.listener_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    fn record_request(&mut self, stream_request: StreamRequest) {
        let service = stream_request.service;
        let command = stream_request.command;
        let keys = stream_request.keys;
        let fields = stream_request.fields;

        match command {
            Command::Add => {
                let service = self.subscriptions.get_mut(&service);
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
                let service = self.subscriptions.get_mut(&service);
                if let Some(s) = service {
                    for key in keys {
                        s.insert(key, fields.clone());
                    }
                }
            }
            Command::Unsubs => {
                let service = self.subscriptions.get_mut(&service);
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

    async fn connect(
        &mut self,
    ) -> anyhow::Result<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>, anyhow::Error>
    {
        let schwab_client_channel = match self.streamer_info.get("schwabClientChannel") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let schwab_client_function_id = match self.streamer_info.get("schwabClientFunctionId") {
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
        let message = self.build_message(Service::Admin, Command::Login, parameters)?;
        write
            .send(Message::Text(message.to_string().into()))
            .await?;

        self.writer = Some(write);

        Ok(read)
    }

    fn build_message(
        &mut self,
        service: Service,
        command: Command,
        parameters: Value,
    ) -> anyhow::Result<Value, anyhow::Error> {
        let schwab_client_customer_id = match self.streamer_info.get("schwabClientCustomerId") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        let schwab_client_correlation_id = match self.streamer_info.get("schwabClientCorrelId") {
            Some(i) => i,
            None => {
                return Err(anyhow!("Unable to read streamer info"));
            }
        };
        self.request_id += 1;
        Ok(json!({
            "service": service.to_string(),
            "command": command.to_string(),
            "requestid": self.request_id,
            "parameters": parameters,
            "SchwabClientCustomerId": schwab_client_customer_id,
            "SchwabClientCorrelId": schwab_client_correlation_id,
        }))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures_util::StreamExt;
    use reqwest::Client;
    use tokio::io::AsyncWriteExt;

    use crate::{SchwabApi, SchwabAuth, schwab::schwab_streamer_api::SchwabStreamerApi};

    #[tokio::test]
    async fn test_conn() -> anyhow::Result<(), anyhow::Error> {
        let reqwest_client = Arc::new(Client::new());
        let schwab_auth = SchwabAuth::new(Arc::clone(&reqwest_client));
        let schwab_api = SchwabApi::new(Arc::clone(&reqwest_client));
        let mut streamer_api = SchwabStreamerApi::new(schwab_api).await?;

        let app_key = std::env::var("SCHWAB_APP_KEY")?;
        let app_secret = std::env::var("SCHWAB_APP_SECRET")?;
        schwab_auth.authorize(&app_key, &app_secret).await?;

        streamer_api.start().await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        streamer_api.stop().await?;
        Ok(())
    }
}
