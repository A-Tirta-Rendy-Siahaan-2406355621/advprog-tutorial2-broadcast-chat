use futures_util::{SinkExt, StreamExt};
use http::Uri;
use std::error::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_websockets::{ClientBuilder, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Connect to WebSocket server
    let (mut ws_stream, _) =
        ClientBuilder::from_uri(Uri::from_static("ws://127.0.0.1:2000"))
            .connect()
            .await?;

    println!(" Connected to websocket server on ws://127.0.0.1:2000");
    println!("Type a message and press Enter (Ctrl+C to exit):");

    let stdin = tokio::io::stdin();
    let mut stdin = BufReader::new(stdin).lines();

    loop {
        tokio::select! {
            // Read from stdin
            line = stdin.next_line() => {
                match line {
                    Ok(Some(text)) => {
                        if !text.trim().is_empty() {
                            ws_stream.send(Message::text(text)).await?;
                        }
                    }
                    Ok(None) => break, // EOF
                    Err(e) => {
                        eprintln!("Error reading stdin: {}", e);
                        break;
                    }
                }
            }

            // Read from WebSocket
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        if let Some(text) = msg.as_text() {
                            println!("Server: {text}");
                        } else if msg.is_close() {
                            println!("Server closed the connection.");
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("WebSocket error: {}", e);
                        break;
                    }
                    None => {
                        println!("WebSocket connection closed by server.");
                        break;
                    }
                }
            }
        }
    }

    println!("Client shutdown.");
    Ok(())
}