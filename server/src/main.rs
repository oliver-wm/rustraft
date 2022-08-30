mod kv_store;

use crate::kv_store::KVStore;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

#[derive(Serialize, Deserialize, Debug)]
enum Status {
    Ok,
    Failure,
}

// #[derive(Serialize, Deserialize, Debug)]
// enum Code {
//     Get {key: i32 },
//     Set {key: i32, value: i32},
//     Del {key: i32 },
// }

#[derive(Serialize, Deserialize, Debug)]
enum Code {
    Get,
    Set,
    Del,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    status: Status,
    value: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    code: Code,
    key: Option<i32>,
    value: Option<i32>,
}

fn handle_client((tcp_stream, _addr): &(TcpStream, SocketAddr)) -> Option<Message> {
    println!("got new connection from {:?}", tcp_stream);

    let mut de = serde_json::Deserializer::from_reader(tcp_stream);

    // Message could be from raft need to be able to call differing functionality based
    // on what comes out of the wire
    let msg = Message::deserialize(&mut de).ok()?;

    println!("msg is {:?}", msg);

    Some(msg)
}

fn main() -> std::io::Result<()> {
    let mut key_val_store = KVStore::new();

    let listener = TcpListener::bind("127.0.0.1:1024")?;

    loop {
        let stream = listener.accept();

        // this part becomes threaded
        match stream {
            Ok(mut stream_pair) => {
                let msg = handle_client(&stream_pair);
                match msg {
                    Some(msg) => {
                        let response = key_val_store.handle_request(&msg);
                        stream_pair
                            .0
                            .write(serde_json::to_string(&response).unwrap().as_bytes())
                            .expect("[SERVER] Failed to write to TcpStream");
                    }
                    None => println!("[SERVER] Failed to handle client msg"),
                }
            }
            Err(e) => {
                println!("[SERVER] Failed accepting socket  {}", e);
            }
        }

        println!("[SERVER] {:?}", key_val_store);
    }
}
