use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::broadcast::{Sender, channel};
use tokio_websockets::{Message, ServerBuilder, WebSocketStream};

type Users = Arc<Mutex<BTreeMap<SocketAddr, String>>>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientMessage {
    message_type: ClientMessageType,
    data: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ClientMessageType {
    Register,
    Message,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerMessage {
    message_type: ServerMessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_array: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum ServerMessageType {
    Users,
    Message,
}

#[derive(Debug, Serialize)]
struct ChatMessage<'a> {
    from: &'a str,
    message: &'a str,
    time: u128,
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn send_broadcast(bcast_tx: &Sender<String>, message: String) {
    let _ = bcast_tx.send(message);
}

fn user_list_message(users: Vec<String>) -> Result<String, serde_json::Error> {
    serde_json::to_string(&ServerMessage {
        message_type: ServerMessageType::Users,
        data: None,
        data_array: Some(users),
    })
}

fn chat_message(from: &str, message: &str) -> Result<String, serde_json::Error> {
    let data = serde_json::to_string(&ChatMessage {
        from,
        message,
        time: now_millis(),
    })?;

    serde_json::to_string(&ServerMessage {
        message_type: ServerMessageType::Message,
        data: Some(data),
        data_array: None,
    })
}

async fn registered_users(users: &Users) -> Vec<String> {
    users.lock().await.values().cloned().collect()
}

async fn sender_name(addr: SocketAddr, users: &Users) -> String {
    users
        .lock()
        .await
        .get(&addr)
        .cloned()
        .unwrap_or_else(|| addr.to_string())
}

async fn broadcast_user_list(
    users: &Users,
    bcast_tx: &Sender<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let message = user_list_message(registered_users(users).await)?;
    send_broadcast(bcast_tx, message);
    Ok(())
}

async fn handle_text_message(
    addr: SocketAddr,
    text: &str,
    users: &Users,
    bcast_tx: &Sender<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    match serde_json::from_str::<ClientMessage>(text) {
        Ok(message) => {
            let ClientMessage { message_type, data } = message;

            match message_type {
                ClientMessageType::Register => {
                    if let Some(nickname) = data.filter(|value| !value.trim().is_empty()) {
                        users.lock().await.insert(addr, nickname.clone());
                        println!("{addr} registered as {nickname}");
                        broadcast_user_list(users, bcast_tx).await?;
                    }
                }
                ClientMessageType::Message => {
                    if let Some(message) = data {
                        let from = sender_name(addr, users).await;
                        println!("Message from {from}: {message}");
                        send_broadcast(bcast_tx, chat_message(&from, &message)?);
                    }
                }
            }
        }
        Err(error) if text.trim_start().starts_with('{') => {
            eprintln!("Invalid YewChat message from {addr}: {error}");
        }
        Err(_) => {
            let from = sender_name(addr, users).await;
            println!("Legacy message from {from}: {text}");
            send_broadcast(bcast_tx, chat_message(&from, text)?);
        }
    }

    Ok(())
}

async fn unregister_user(
    addr: SocketAddr,
    users: &Users,
    bcast_tx: &Sender<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let removed_user = users.lock().await.remove(&addr);

    if let Some(nickname) = removed_user {
        println!("{nickname} disconnected from {addr}");
        broadcast_user_list(users, bcast_tx).await?;
    } else {
        println!("Client {addr} disconnected");
    }

    Ok(())
}

async fn handle_connection(
    addr: SocketAddr,
    mut ws_stream: WebSocketStream<TcpStream>,
    bcast_tx: Sender<String>,
    users: Users,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut bcast_rx = bcast_tx.subscribe();

    loop {
        tokio::select! {
            incoming_message = ws_stream.next() => {
                match incoming_message {
                    Some(Ok(message)) => {
                        if let Some(text) = message.as_text() {
                            handle_text_message(addr, text, &users, &bcast_tx).await?;
                        } else if message.is_close() {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        eprintln!("WebSocket error from {addr}: {error}");
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }

            broadcast_message = bcast_rx.recv() => {
                match broadcast_message {
                    Ok(message) => {
                        if ws_stream.send(Message::text(message)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!("{addr} skipped {skipped} broadcast messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }

    unregister_user(addr, &users, &bcast_tx).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (bcast_tx, _) = channel(16);
    let users = Arc::new(Mutex::new(BTreeMap::new()));

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await?;
    println!("Server is listening on ws://{addr}");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client connected from {addr}");

        let bcast_tx = bcast_tx.clone();
        let users = users.clone();

        tokio::spawn(async move {
            let (_request, ws_stream) = ServerBuilder::new().accept(socket).await?;
            handle_connection(addr, ws_stream, bcast_tx, users).await
        });
    }
}
