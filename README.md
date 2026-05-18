## Experiment 2.1: Original code, and how it run

### Description

In this experiment, I implemented the original broadcast chat application using asynchronous Rust. The application consists of one websocket server and multiple websocket clients. The server listens on `127.0.0.1:2000`, while every client connects to the same websocket address. When one client sends a message, the server receives that message and broadcasts it to all connected clients.

### How to run

Run the server first:

```bash
cargo run --bin server
```

Then run three clients in three different terminals:

```bash
cargo run --bin client
```

After that, type a message in one of the clients and press Enter.

### Result

![Experiment 2.1 - Server and three clients running](screenshots/experiment-2.1-server.png)
![Experiment 2.1 - Server and three clients running](screenshots/experiment-2.1-client1.png)
![Experiment 2.1 - Server and three clients running](screenshots/experiment-2.1-client2.png)
![Experiment 2.1 - Server and three clients running](screenshots/experiment-2.1-client3.png)


### Explanation

The program uses asynchronous programming because the server needs to handle many clients at the same time without blocking the whole application. The server accepts websocket connections using `TcpListener`, then spawns a new asynchronous task for each connected client using `tokio::spawn`. Inside each connection handler, `tokio::select!` is used to wait for two possible events: a message from the current websocket client or a broadcast message from another client. This means the server can receive and send messages concurrently.

The broadcast mechanism uses `tokio::sync::broadcast`. When a client sends a message, the server forwards that message into the broadcast channel. Every active client connection has its own receiver from `bcast_tx.subscribe()`, so every client can receive the same message. This is why a message typed in one client appears in the other clients. This behavior shows why asynchronous programming is useful for chat applications, because the application needs to wait for network input from many sources at the same time.

---