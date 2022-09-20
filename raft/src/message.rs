use crate::log::{LogEntry, Value};
use crate::state_machine::{RaftState, Role};

// another idea?
// pub trait MessageHandler {
//     fn handle(self, &mut raft_state, RaftState) -> MessageType;
// }

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct Io {
    pub(crate) src: usize,
    pub(crate) dst: usize,
}

#[derive(PartialEq, Debug)]
pub enum MessageType {
    ClientRequestMsg(ClientRequest),
    RequestVoteMsg(RequestVote),
    BecomeCandidate(Vec<RequestVote>),
    RequestVoteResponseMsg(RequestVoteResponse),
    AppendEntriesMsg(AppendEntries),
    AppendEntriesResponseMsg(AppendEntriesResponse),
    UpdateFollowerMsg(AppendEntries),
    HeartBeatMsg(HeartBeat), // UpdateFollowers
    NoneMsg,
}

#[derive(PartialEq, Debug)]
pub struct UpdateFollower {
    pub node: usize,
    pub prev_index: i32,
    pub prev_term: usize,
    pub term: usize,
}

impl UpdateFollower {
    pub fn handle(&self, raft_state: &mut RaftState) -> MessageType {
        assert_eq!(raft_state.role, Role::Leader);
        MessageType::NoneMsg
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct ClientRequest {
    pub(crate) dst: usize,
    pub(crate) value: Value,
}

impl ClientRequest {
    pub fn handle(&self, raft_state: &mut RaftState) -> MessageType {
        let (prev_index, prev_term) = if raft_state.log.log.len() == 0 {
            (None, None)
        } else {
            (
                Some(raft_state.log.log.len() - 1),
                Some(raft_state.log.log.len() - 1),
            )
        };

        let entry = LogEntry {
            term: raft_state.current_term,
            value: self.value,
        };
        raft_state
            .log
            .append_entries(&mut vec![entry], prev_term, prev_index);

        MessageType::NoneMsg
    }
}

pub struct ClientResponse {}

impl ClientResponse {}

type Vote = usize;
type Machine = usize;

#[derive(PartialEq, Debug)]
pub struct RequestVote {
    pub(crate) io: Io,
    // pub(crate) votes_responded: HashMap<Machine, Vote>,
    pub(crate) term: Option<usize>,
    pub(crate) last_term: Option<usize>,
    pub(crate) last_index: Option<usize>,
}

impl RequestVote {
    pub fn handle(&self, raft_state: &mut RaftState) -> MessageType {
        assert_eq!(raft_state.role, Role::Candidate);
        assert_eq!(raft_state.votes_received.get(&self.io.dst), None);
        assert!(self.last_term >= raft_state.current_term);

        if raft_state.check_term(&self.term).is_err() {
            return MessageType::NoneMsg;
        }

        let mut vote_granted;
        if raft_state.votes_received.get(&self.io.dst).is_none() {
            vote_granted = false;
        } else {
            let log_len = raft_state.log.log.len();
            let (server_last_index, server_last_term) =
                raft_state.get_prev_term_from_log_len(log_len);

            if server_last_term > self.last_term {
                vote_granted = false;
            } else if server_last_term < self.last_term {
                vote_granted = true;
            } else if server_last_index > self.last_index {
                vote_granted = false;
            } else {
                vote_granted = true;
            }
        }

        let reply = RequestVoteResponse {
            io: Io {
                src: self.io.dst,
                dst: self.io.src,
            },
            term: raft_state.current_term,
            vote_granted,
        };
        MessageType::RequestVoteResponseMsg(reply)
    }
}
#[derive(PartialEq, Debug)]
pub struct RequestVoteResponse {
    pub io: Io,
    pub term: Option<usize>,
    pub vote_granted: bool,
}

impl RequestVoteResponse {
    fn handle(&self, raft_state: &mut RaftState) -> MessageType {
        if raft_state.check_term(&self.term).is_err() {
            return MessageType::NoneMsg;
        }

        if self.vote_granted {
            raft_state.votes_received.insert(self.io.src, 1);
        }

        if raft_state.role == Role::Candidate
            && raft_state.votes_received.values().len() >= raft_state.votes_received.len() / 2
        {
            raft_state.become_leader();
        }

        MessageType::NoneMsg
    }
}
#[derive(PartialEq, Debug)]
pub struct AppendEntries {
    pub(crate) io: Io,
    pub(crate) term: Option<usize>,
    pub(crate) prev_index: Option<usize>,
    pub(crate) prev_term: Option<usize>,
    pub(crate) entries: Vec<LogEntry>,
    //commit_index
}

impl AppendEntries {
    pub fn handle(&mut self, raft_state: &mut RaftState) -> MessageType {
        if raft_state.check_term(&self.term).is_err() {
            return MessageType::NoneMsg;
        }

        let success =
            raft_state
                .log
                .append_entries(&mut self.entries, self.prev_term, self.prev_index);

        let match_index: Option<usize> = if success {
            match self.prev_index {
                None => Some(0),
                Some(ind) => Some(ind + self.entries.len()),
            }
        } else {
            None
        };

        MessageType::AppendEntriesResponseMsg(AppendEntriesResponse {
            io: Io {
                src: self.io.dst,
                dst: self.io.src,
            },
            term: raft_state.current_term,
            success,
            match_index,
        })
    }
}
#[derive(PartialEq, Debug)]
pub struct AppendEntriesResponse {
    pub(crate) io: Io,
    pub(crate) term: Option<usize>,
    pub success: bool,
    pub(crate) match_index: Option<usize>,
}

impl AppendEntriesResponse {
    pub fn handle(&self, raft_state: &mut RaftState) -> MessageType {
        assert_eq!(raft_state.role, Role::Leader);
        assert_eq!(self.term, raft_state.current_term);
        if raft_state.check_term(&self.term).is_err() {
            return MessageType::NoneMsg;
        }
        // Note options are returned from hashmaps, so last_index from log will be unwrapped to be stored
        // also means if it's not in the dict it's empty
        if self.success {
            raft_state.insert_next_index(&self.io.src, &self.match_index);
            raft_state.insert_match_index(&self.io.src, &self.match_index);

            if raft_state.match_index.len() > 0 {
                // not sure about this block
                let mut values: Vec<usize> = raft_state.match_index.values().cloned().collect();
                values.sort();
                let new_commit_index = values[raft_state.match_index.len() / 2];
                if raft_state.commit_index.is_some()
                    && new_commit_index > raft_state.commit_index.unwrap()
                {
                    raft_state.commit_index = Some(new_commit_index);
                    // send application msg here
                }
            }
        } else if raft_state.next_index.get(&self.io.src).unwrap() > &(0 as usize) {
            raft_state.next_index.insert(
                self.io.src,
                raft_state.next_index.get(&self.io.src).unwrap() - 1,
            );
        }

        MessageType::NoneMsg
    }
}

#[derive(PartialEq, Debug)]
pub struct HeartBeat {
    pub(crate) node: usize,
}

impl HeartBeat {
    pub fn handle(&self, raft_state: &mut RaftState) -> MessageType {
        if raft_state.role == Role::Follower {
            return MessageType::NoneMsg;
        }
        assert_eq!(raft_state.role, Role::Leader);

        // does this need a check term call here ?

        for node in raft_state.nodes.iter() {
            if !raft_state.next_index.contains_key(&node) {
                raft_state
                    .next_index
                    .insert(*node, raft_state.log.log.len());
            }
        }

        if raft_state.node_number != self.node {
            let (prev_index, prev_term) = raft_state.get_prev(&self.node);
            let mut entries = raft_state.get_entries_from_prev_index(prev_index);
            let msg = AppendEntries {
                io: Io {
                    src: raft_state.node_number,
                    dst: self.node,
                },
                term: raft_state.current_term,
                prev_index,
                prev_term,
                entries: entries.clone(),
            };
            return MessageType::UpdateFollowerMsg(msg);
        }

        MessageType::NoneMsg
    }
}

#[cfg(test)]
mod tests {
    use crate::log::LogEntry;
    use crate::message::{AppendEntries, AppendEntriesResponse, ClientRequest, Io};
    use crate::MessageType::{ClientRequestMsg, HeartBeatMsg, NoneMsg, UpdateFollowerMsg};
    use crate::{HeartBeat, MessageType, RaftState};

    #[test]
    fn test_append_entries() {
        let mut rf = RaftState::new(0, &vec![1, 2, 3]);

        let mut ae = MessageType::AppendEntriesMsg(AppendEntries {
            io: Io { src: 1, dst: 0 },
            term: rf.current_term,
            prev_index: None,
            prev_term: None,
            entries: vec![LogEntry {
                term: rf.current_term,
                value: (0, 0),
            }],
        });

        let resp = rf.handle_message(&mut ae);

        let aer = AppendEntriesResponse {
            io: Io { src: 0, dst: 1 },
            term: rf.current_term,
            success: true,
            match_index: Some(0),
        };

        assert_eq!(resp, MessageType::AppendEntriesResponseMsg(aer));
    }

    #[test]
    fn test_client_request() {
        let rf = application_append();
        assert_eq!(
            rf.log.log[0],
            LogEntry {
                term: rf.current_term,
                value: (0, 0)
            }
        );
    }

    fn application_append() -> RaftState {
        let mut rf = RaftState::new(0, &vec![1, 2, 3]);
        let mut cr = ClientRequest {
            dst: 0,
            value: (0, 0),
        };

        rf.handle_message(&mut ClientRequestMsg(cr));
        return rf;
    }

    #[test]
    fn test_update_follower() {
        let mut rs = application_append();
        let mut responses: Vec<MessageType> = Vec::new();

        assert_eq!(rs.current_term, Some(0));

        for i in 0..3 {
            let mut hbm = HeartBeat { node: i };
            let resp = rs.handle_message(&mut HeartBeatMsg(hbm));
            responses.push(resp);
        }

        assert_eq!(rs.current_term, Some(0));

        let expected = vec![
            NoneMsg,
            UpdateFollowerMsg(AppendEntries {
                io: Io { src: 0, dst: 1 },
                term: rs.current_term,
                prev_index: Some(0),
                prev_term: None,
                entries: vec![],
            }),
            UpdateFollowerMsg(AppendEntries {
                io: Io { src: 0, dst: 2 },
                term: rs.current_term,
                prev_index: Some(0),
                prev_term: None,
                entries: vec![],
            }),
        ];
        assert_eq!(responses, expected);
        assert_eq!(rs.next_index.len(), 3);
    }
}
