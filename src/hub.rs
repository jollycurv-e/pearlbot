use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{debug, info, warn, error};

use crate::config::McUuid;

#[derive(Debug, Clone, Deserialize)]
struct InboundMessage {
    action: String,
    data: Value,
}

#[derive(Debug, Clone, Serialize)]
struct OutboundMessage {
    action: String,
    data: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PearlRequest {
    pub slot: u8,
    pub requester: String,
    pub requester_uuid: McUuid,
}

#[derive(Debug, Clone, Serialize)]
pub struct PearlResult {
    pub slot: u8,
    pub success: bool,
    pub message: String,
    pub requester: String,
}

#[derive(Debug, Clone)]
pub enum HubEvent {
    Open,
    PearlRequest(PearlRequest),
    Close(String),
    Error(String),
    Unknown(String),
}

#[derive(Clone)]
pub struct HubClient {
    sender: mpsc::UnboundedSender<Message>,
    events: broadcast::Sender<HubEvent>,
}

impl HubClient {
    pub async fn connect(url: String, api_key: String) -> Result<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();
        let (events, _) = broadcast::channel(64);
        let events_for_task = events.clone();
        let task_url = url.clone();

        tokio::spawn(async move {
            let mut reconnect_count = 0_u32;
            loop {
                let request_url = format!(
                    "{}/websocket/connect",
                    task_url.trim_end_matches('/')
                );

                debug!("[hub] connecting to {request_url} (attempt {})", reconnect_count + 1);
                let connect_start = Instant::now();

                let request = match build_request(&request_url, &api_key) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("[hub] build request failed: {e}");
                        events_for_task.send(HubEvent::Error(format!("build request: {e}"))).ok();
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                match connect_async(request).await {
                    Ok((socket, response)) => {
                        info!("[hub] connected in {:.1}ms — HTTP {}",
                            connect_start.elapsed().as_secs_f32() * 1000.0,
                            response.status());
                        events_for_task.send(HubEvent::Open).ok();
                        reconnect_count = 0;

                        let (mut write, mut read) = socket.split();
                        let mut keepalive = interval(Duration::from_secs(5));

                        loop {
                            tokio::select! {
                                msg = receiver.recv() => {
                                    match msg {
                                        Some(m) => {
                                            debug!("[hub] sending: {} bytes", m.len());
                                            if let Err(e) = write.send(m).await {
                                                error!("[hub] send error: {e}");
                                                events_for_task.send(HubEvent::Error(e.to_string())).ok();
                                                break;
                                            }
                                        }
                                        None => {
                                            info!("[hub] sender dropped — exiting WS task");
                                            return;
                                        }
                                    }
                                }
                                _ = keepalive.tick() => {
                                    debug!("[hub] sending keepalive ping");
                                    if let Err(e) = write.send(Message::Ping("ping".as_bytes().to_vec().into())).await {
                                        error!("[hub] keepalive ping failed: {e}");
                                        events_for_task.send(HubEvent::Error(e.to_string())).ok();
                                        break;
                                    }
                                }
                                msg = read.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            debug!("[hub] recv text ({} bytes): {}", text.len(), &text[..text.len().min(200)]);
                                            let event = parse_message(&text);
                                            match &event {
                                                HubEvent::PearlRequest(req) => info!("[hub] pearl_request: slot={} requester={} uuid={}", req.slot, req.requester, req.requester_uuid),
                                                HubEvent::Open => info!("[hub] API key accepted"),
                                                HubEvent::Error(e) => error!("[hub] message parse error: {e}"),
                                                HubEvent::Unknown(msg) => {
                                                    let preview = msg.chars().take(120).collect::<String>();
                                                    debug!("[hub] ignoring unrelated websocket message: {preview}");
                                                }
                                                _ => {}
                                            }
                                            events_for_task.send(event).ok();
                                        }
                                        Some(Ok(Message::Pong(_))) => {
                                            debug!("[hub] recv pong");
                                        }
                                        Some(Ok(Message::Close(frame))) => {
                                            let reason = frame
                                                .map(|f| format!("code={} reason={}", f.code, f.reason))
                                                .unwrap_or_else(|| "no frame".to_owned());
                                            warn!("[hub] server closed WS: {reason}");
                                            events_for_task.send(HubEvent::Close(reason)).ok();
                                            break;
                                        }
                                        Some(Ok(other)) => {
                                            debug!("[hub] recv non-text frame: {:?}", std::mem::discriminant(&other));
                                        }
                                        Some(Err(e)) => {
                                            error!("[hub] WS read error: {e}");
                                            events_for_task.send(HubEvent::Error(e.to_string())).ok();
                                            break;
                                        }
                                        None => {
                                            warn!("[hub] WS stream ended");
                                            events_for_task.send(HubEvent::Close("stream ended".to_owned())).ok();
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        info!("[hub] disconnected after {:.1}s total",
                            connect_start.elapsed().as_secs_f32());
                    }
                    Err(e) => {
                        error!("[hub] connect failed in {:.1}ms: {e}",
                            connect_start.elapsed().as_secs_f32() * 1000.0);
                        events_for_task.send(HubEvent::Error(e.to_string())).ok();
                    }
                }

                reconnect_count = reconnect_count.saturating_add(1);
                let delay = if reconnect_count >= 5 { 60 } else { 5 };
                warn!("[hub] reconnecting in {delay}s (attempt {reconnect_count}) url={} api_key_present={}",
                    task_url, !api_key.is_empty());
                events_for_task.send(HubEvent::Close(format!(
                    "reconnecting in {delay}s (attempt {reconnect_count})"
                ))).ok();
                sleep(Duration::from_secs(delay)).await;
            }
        });

        Ok(Self { sender, events })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.events.subscribe()
    }

    pub fn send_pearl_result(&self, result: PearlResult) -> Result<()> {
        let payload = OutboundMessage {
            action: "pearl_result".to_owned(),
            data: serde_json::to_value(&result)?,
        };
        let text = serde_json::to_string(&payload)?;
        debug!("[hub] sending pearl_result: {text}");
        self.sender
            .send(Message::Text(text.into()))
            .map_err(|e| anyhow!("send failed: {e}"))?;
        info!("[hub] pearl_result sent — slot={} success={} requester={}",
            result.slot, result.success, result.requester);
        Ok(())
    }
}

fn build_request(url: &str, api_key: &str) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    use tokio_tungstenite::tungstenite::http::header::HeaderValue;
    let mut request = url
        .to_owned()
        .into_client_request()
        .map_err(|e| anyhow!("invalid URL: {e}"))?;
    let headers = request.headers_mut();
    headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
    headers.insert("client-type", HeaderValue::from_static("pearlbot"));
    headers.insert("mc_server", HeaderValue::from_static("pearlbot"));
    debug!("[hub] built request for {url} with client-type=pearlbot mc_server=pearlbot");
    Ok(request)
}

fn parse_message(text: &str) -> HubEvent {
    let Ok(msg) = serde_json::from_str::<InboundMessage>(text) else {
        return HubEvent::Unknown(text.to_owned());
    };
    match msg.action.as_str() {
        "pearl_request" => match serde_json::from_value::<PearlRequest>(msg.data) {
            Ok(req) => HubEvent::PearlRequest(req),
            Err(e) => HubEvent::Error(format!("bad pearl_request: {e}")),
        },
        "key-accepted" => HubEvent::Open,
        _ => HubEvent::Unknown(text.to_owned()),
    }
}
