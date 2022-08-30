extern crate core;

use crate::log::Log;
use crate::message::{HeartBeat, MessageType};
use crate::state_machine::{RaftState, Role};
use crate::transport::{Node, Transport};
use crossbeam::queue::SegQueue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::Thread;
use std::time::Duration;

mod log;
mod message;
mod state_machine;
mod transport;

const HEARTBEAT_MILLIS: u64 = 300;

struct RaftServer {
    transport: Transport,
    raft: Arc<Mutex<RaftState>>,
    in_queue: Arc<SegQueue<MessageType>>,
    out_queue: Arc<SegQueue<MessageType>>,
    workers: Vec<Thread>,
    nodes: Vec<usize>,
}

impl RaftServer {
    fn new(nodes: &Vec<Node>) -> RaftServer {
        RaftServer {
            transport: Transport {},
            raft: Arc::new(Mutex::new(RaftState {
                role: Role::Leader,
                node_number: 0,
                current_term: None,
                votes_received: HashMap::new(),
                log: Log::new(),
                commit_index: None,
                last_applied: None,
                nodes: nodes.clone(),
                next_index: Default::default(),
                match_index: Default::default(),
            })),
            in_queue: Arc::new(SegQueue::new()),
            out_queue: Arc::new(SegQueue::new()),
            workers: vec![],
            nodes: nodes.clone(),
        }
    }

    fn init() {
        // some sort of configuration of server -> node number done here
    }

    fn shutdown() {
        // kill all workers gracefully - persist log
    }

    fn run(&mut self) {
        println!("starting raft worker");
        let mut raft = self.raft.clone();
        let mut in_q = self.in_queue.clone();
        let mut out_q = self.out_queue.clone();
        let hi = thread::spawn(|| Self::raft_worker(raft, in_q, out_q));
        println!("starting hb worker");

        let mut in_q = self.in_queue.clone();
        let mut nodes = self.nodes.clone();
        thread::spawn(|| Self::heartbeat_worker(in_q, nodes));
        println!("starting recv worker");

        let mut in_q = self.in_queue.clone();
        thread::spawn(|| Self::recv_worker(in_q));
        println!("starting send worker");

        let mut out_q = self.out_queue.clone();
        thread::spawn(|| Self::send_worker(out_q));
    }

    fn raft_worker(
        raft: Arc<Mutex<RaftState>>,
        in_queue: Arc<SegQueue<MessageType>>,
        out_queue: Arc<SegQueue<MessageType>>,
    ) {
        loop {
            if let Some(mut msg) = in_queue.pop() {
                println!("raft worker received {:?}", msg);
                let out_msg = raft.lock().unwrap().handle_message(&mut msg);
                println!("raft response {:?}", out_msg);
                out_queue.push(out_msg);
            }
        }
    }

    // Only runs on leader, need a mechanism to stop / start this
    fn heartbeat_worker(in_queue: Arc<SegQueue<MessageType>>, nodes: Vec<usize>) {
        loop {
            println!("heartbeat worker beating");
            thread::sleep(Duration::from_millis(HEARTBEAT_MILLIS));
            for node in nodes.iter() {
                in_queue.push(MessageType::HeartBeatMsg(HeartBeat { node: *node }));
            }
        }
    }

    fn recv_worker(in_queue: Arc<SegQueue<MessageType>>) {}

    fn send_worker(out_queue: Arc<SegQueue<MessageType>>) {
        loop {
            if let Some(msg) = out_queue.pop() {
                println!("send worker received {:?}", msg);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        let mut rf = RaftServer::new(&vec![1, 2, 3]);
        // note: as long as rf is in scope threads won't be dropped, I guess this will be the handle to the client
        rf.run();

        // stdout LGTM lol
        // thread::sleep(Duration::from_secs(60));
    }
}
