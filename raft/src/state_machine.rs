use crate::log::{Log, LogEntry};
use crate::message::{Io, MessageType, RequestVote};
use crate::MessageType::BecomeCandidate;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;

// 150–300ms - Timeouts chosen randomly between this interval
// leader >= candidate => candidate becomes follower

#[derive(Debug)]
pub struct RaftState {
    pub role: Role,
    pub node_number: usize,
    pub current_term: Option<usize>,
    pub votes_received: HashMap<usize, usize>,
    pub log: Log,
    pub commit_index: Option<usize>,
    pub last_applied: Option<usize>,

    //state tracking of cluster
    pub nodes: Vec<usize>,
    pub next_index: HashMap<usize, usize>,
    pub match_index: HashMap<usize, usize>,
}

#[derive(Debug, PartialEq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

impl RaftState {
    pub(crate) fn new(node: usize, nodes: &Vec<usize>) -> RaftState {
        RaftState {
            role: Role::Leader,
            node_number: node,
            current_term: Some(0),
            votes_received: HashMap::new(),
            log: Log::new(),
            commit_index: None,
            last_applied: None,

            nodes: nodes.clone(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    pub fn handle_message(&mut self, msg: &mut MessageType) -> MessageType {
        // TODO figure out a way to do this dispatch with generic types (something like GADTS or traits)
        return match msg {
            MessageType::ClientRequestMsg(cr) => cr.handle(self),
            MessageType::UpdateFollowerMsg(uf) => uf.handle(self),
            MessageType::AppendEntriesMsg(ae) => ae.handle(self),
            MessageType::AppendEntriesResponseMsg(aer) => aer.handle(self),
            MessageType::RequestVoteMsg(_) => {
                todo!()
            }
            MessageType::RequestVoteResponseMsg(_) => {
                todo!()
            }
            MessageType::NoneMsg => MessageType::NoneMsg,
            MessageType::HeartBeatMsg(hb) => hb.handle(self),
            MessageType::BecomeCandidate(_) => self.become_candidate(),
        };
    }

    pub(crate) fn check_term(&mut self, term: &Option<usize>) -> Result<(), ()> {
        return match (*term).cmp(&self.current_term) {
            Ordering::Greater => {
                self.become_follower();
                self.current_term = *term;
                self.votes_received.clear();
                Ok(())
            }
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(()),
        };
    }

    fn become_follower(&mut self) {
        self.role = Role::Follower;
    }

    fn become_candidate(&mut self) -> MessageType {
        self.role = Role::Candidate;
        // if self.current_term.is_some() {
        //     self.current_term += 1;
        // }else {
        //     self.current_term = Some(0);
        // };
        // self.current_term.unwrap() += 1;
        // need self.voted_for
        self.votes_received.drain();

        let (last_index, last_term) = self.get_prev_term_from_log_len(self.log.log.len());

        let mut messages = Vec::new();
        for follower in self.nodes.iter_mut() {
            let rv = RequestVote {
                io: Io {
                    src: self.node_number,
                    dst: *follower,
                },
                term: self.current_term,
                last_term,
                last_index,
            };
            messages.push(rv);
        }

        BecomeCandidate(messages)
    }

    pub(crate) fn get_prev_term_from_log_len(
        &mut self,
        log_len: usize,
    ) -> (Option<usize>, Option<usize>) {
        let server_last_index;
        if log_len == 0 {
            server_last_index = None;
        } else {
            server_last_index = Some(log_len - 1);
        }

        let server_last_term;
        if server_last_index.is_some() {
            server_last_term = self.log.log[server_last_index.unwrap()].term;
        } else {
            server_last_term = None;
        }
        (server_last_index, server_last_term)
    }

    pub(crate) fn become_leader(&mut self) {
        self.role = Role::Leader;

        self.next_index.drain();
        self.match_index.drain();
        for follower in self.nodes.iter_mut() {
            self.next_index.insert(*follower, self.log.log.len());
        }
    }

    pub(crate) fn insert_match_index(&mut self, src: &usize, match_index: &Option<usize>) {
        if let Some(ind) = match_index {
            self.match_index.insert(*src, *ind);
        } // else not in hashmap (None)
    }

    pub(crate) fn insert_next_index(&mut self, src: &usize, match_index: &Option<usize>) {
        if let Some(ind) = match_index {
            self.match_index.insert(*src, *ind + 1);
        }
    }

    pub(crate) fn get_entries_from_prev_index(
        &mut self,
        prev_index: Option<usize>,
    ) -> Vec<LogEntry> {
        let mut result: Vec<LogEntry> = vec![];
        return match prev_index {
            None => result,
            Some(index) => {
                for (i, val) in self.log.log.iter().enumerate() {
                    if i > index {
                        result.push(val.clone());
                    }
                }
                // result.extend(self.log.log[index + 1..])
                result
            }
        };
    }

    pub(crate) fn get_prev(&self, node: &usize) -> (Option<usize>, Option<usize>) {
        let mut prev_index = None;
        let mut prev_term = None;

        let next_index = self.next_index.get(node);
        if next_index.is_some() && *next_index.unwrap() != 0 as usize {
            prev_index = Some(*next_index.unwrap() - 1);
            prev_term = self.log.log[*next_index.unwrap() - 1].term;

            if let Some(pt) = prev_term {
                if pt == 0 {
                    prev_term = None;
                } else {
                    prev_term = Some(pt - 1);
                }
            }
        }

        (prev_index, prev_term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogEntry;
    use crate::message;
    use crate::message::{AppendEntries, AppendEntriesResponse, Io};
    use crate::MessageType::AppendEntriesResponseMsg;

    #[test]
    fn test_something() {
        let mut rf = RaftState::new(0, &vec![1, 2, 3]);
        println!("wow {:?}", rf);
        let msg = message::ClientRequest {
            dst: 2,
            value: (0, 0),
        };
        assert_eq!(
            rf.handle_message(&mut MessageType::ClientRequestMsg(msg)),
            MessageType::NoneMsg
        );
    }

    #[test]
    fn test_log_replication() {
        let mut nodes = &vec![0, 1, 2];
        let state1 = &mut RaftState::new(0, nodes);
        let state2 = &mut RaftState::new(0, nodes);
        let state3 = &mut RaftState::new(0, nodes);

        let mut raft_states = vec![state1, state2, state3];

        for mut state in raft_states.iter_mut() {
            let mut ae = MessageType::AppendEntriesMsg(AppendEntries {
                io: Io { src: 0, dst: 1 },
                term: Some(0),
                prev_index: None,
                prev_term: None,
                entries: vec![LogEntry {
                    term: state.current_term,
                    value: (0, 0),
                }],
            });
            let aer = state.handle_message(&mut ae);
            let resp = AppendEntriesResponseMsg(AppendEntriesResponse {
                io: Io { src: 1, dst: 0 },
                term: Some(0),
                success: true,
                match_index: Some(0),
            });
            assert_eq!(resp, aer);
        }

        assert_eq!(raft_states[0].log.log, raft_states[1].log.log,);
        assert_eq!(raft_states[1].log.log, raft_states[2].log.log,);
    }

    #[test]
    fn test_consensus() {}

    #[test]
    fn test_election() {}
}
