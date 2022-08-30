use crate::{Code, Message, Response, Status};
use std::collections::HashMap;

#[derive(Debug)]
pub struct KVStore {
    store: HashMap<i32, i32>,
}

impl KVStore {
    pub fn new() -> KVStore {
        KVStore {
            store: HashMap::new(),
        }
    }

    pub fn handle_request(&mut self, msg: &Message) -> Response {
        let resp = match msg.code {
            Code::Get => {
                if msg.key.is_some() && self.store.contains_key(&msg.key.unwrap()) {
                    Some(Response {
                        status: Status::Ok,
                        value: Some(*self.store.get(&msg.key.unwrap()).unwrap()),
                    })
                } else {
                    None
                }
            }
            Code::Set => {
                if msg.key.is_none() || msg.value.is_none() {
                    None
                } else {
                    self.store.insert(msg.key.unwrap(), msg.value.unwrap());
                    Some(Response {
                        status: Status::Ok,
                        value: None,
                    })
                }
            }
            Code::Del => {
                if msg.key.is_some() && self.store.contains_key(&msg.key.unwrap()) {
                    self.store.remove(&msg.key.unwrap());
                    Some(Response {
                        status: Status::Ok,
                        value: None,
                    })
                } else {
                    None
                }
            }
        };

        match resp {
            Some(r) => r,
            None => Response {
                status: Status::Failure,
                value: None,
            },
        }
    }
}
