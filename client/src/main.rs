use clap::Parser;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::net::TcpStream;
use std::str::FromStr;
use std::string::ParseError;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Code {
    Get,
    Set,
    Del,
}

impl FromStr for Code {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let m = match s {
            "get" => Ok(Code::Get),
            "set" => Ok(Code::Set),
            "del" => Ok(Code::Del),
            &_ => todo!(),
        };
        m
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Status {
    Ok,
    Failure,
}

#[derive(Parser, Serialize, Deserialize, Debug)]
struct Message {
    #[clap(short, long, value_parser)]
    code: Code,
    #[clap(short, long, value_parser)]
    key: Option<i32>,
    #[clap(short, long, value_parser)]
    value: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    status: Status,
    value: Option<i32>,
}

fn send_message(msg: String) {
    let mut stream = TcpStream::connect("127.0.0.1:1024").expect("help");
    match stream.write(msg.as_bytes()) {
        Ok(..) => {}
        Err(e) => {
            println!("{}", e);
            return;
        }
    }

    let mut de = serde_json::Deserializer::from_reader(stream);
    let response = Response::deserialize(&mut de);
    match response {
        Ok(response) => println!("{:?}", response),
        Err(e) => {
            println!("{}", e);
            return;
        }
    }
}

// fn main() {
//     use std::sync::{Mutex, Arc};
// use std::thread;

fn main() {
    let msg = Message::parse();
    send_message(serde_json::to_string(&msg).expect("help"));
}
// let x = 1;
// let thing = Arc::new(Mutex::new(x));
//
// for _i in 0..10 {
//     let mut blee = Arc::clone(&thing);
//     let h = thread::spawn(move || {
//         blah(&mut blee);
//     });
//     println!("{:?}", thing);
//     h.join();
// }
//
// println!("{:?}", x);

// fn blah(blee: &mut Arc<Mutex<i32>>) {
//     *blee.lock().unwrap() += 1;
// }
