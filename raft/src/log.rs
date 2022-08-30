use std::fmt::Debug;

pub type Value = (i32, i32);

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct LogEntry {
    pub(crate) term: Option<usize>,
    pub(crate) value: Value,
}

#[derive(Debug)]
pub struct Log {
    pub(crate) log: Vec<LogEntry>,
}

// TODO rewrite for generic types? User specified and loaded from config possibly
impl Log {
    pub(crate) fn new() -> Log {
        Log { log: Vec::new() }
    }

    pub(crate) fn append_entries(
        &mut self,
        entries: &mut Vec<LogEntry>,
        prev_term: Option<usize>,
        prev_index: Option<usize>,
    ) -> bool {
        //first entry is special case
        if prev_index.is_none() {
            self.update_log(entries, 0);
            return true;
        }

        //all subsequent entries
        let prev_index = prev_index.unwrap();

        if prev_index >= self.log.len() {
            return false;
        }

        if self.log[prev_index].term != prev_term {
            return false;
        }

        self.update_log(entries, prev_index + 1);

        return true;
    }

    fn update_log(&mut self, entries: &mut Vec<LogEntry>, index: usize) {
        // delete the things if there is a mismatch
        // (need to learn a better way to zip two iterators from a certain position)
        let mut iter = self.log.iter();
        for _ in 0..index {
            iter.next();
        }

        let zipped = iter.zip(entries.iter());
        for (i, (e1, e2)) in zipped.enumerate() {
            if e1.term != e2.term {
                self.log.drain(index + i..);
                break;
            }
        }

        // actual update logic
        for (pos, entry_ref) in entries.iter().enumerate() {
            if index + pos == self.log.len() {
                self.log.push((*entry_ref).clone());
            } else {
                self.log[index + pos] = (*entry_ref).clone();
            }
        }

        //persist data here!! write to file or sqlite
    }
}

// TODO: refactor for Some(term)
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_empty_true() {
        let mut log: Log = Log::new();
        let ret = log.append_entries(&mut vec![], Some(1), None);
        assert_eq!(ret, true);

        let mut vec = add_terms();
        let mut ret = log.append_entries(&mut vec, Some(1), None);
        assert_eq!(ret, true);
        assert_eq!(log.log.len(), 100);
        assert_eq!(log.log, add_terms());
        let ret = log.append_entries(&mut vec![], Some(1), None);
        assert_eq!(ret, true);
        assert_eq!(log.log.len(), 100);
        assert_eq!(log.log, add_terms());

        let ret = log.append_entries(&mut vec![], Some(10), Some(10));
        assert_eq!(ret, true);
        assert_eq!(log.log.len(), 100);
        assert_eq!(log.log, add_terms());
    }

    #[test]
    fn test_empty_false() {
        let mut log: Log = Log::new();
        let ret = log.append_entries(&mut vec![], Some(1), Some(1));
        assert_eq!(ret, false);
    }

    #[test]
    fn test_delete_if_term_is_different() {
        // must return TRUE if conflicting entry replaced
        let mut log: Log = Log::new();

        let mut vec = add_terms();
        let mut ret = log.append_entries(&mut vec, Some(1), None);
        assert_eq!(ret, true);

        println!("{:?}", log.log);

        ret = log.append_entries(
            &mut vec![
                LogEntry {
                    value: (10000, 1),
                    term: Some(1),
                },
                LogEntry {
                    value: (10000, 1),
                    term: Some(78),
                },
            ],
            Some(0),
            Some(0),
        );
        println!("{:?}", log.log);

        assert_eq!(ret, true);
        assert_eq!(log.log.len(), 3);
        assert_eq!(
            log.log[2],
            LogEntry {
                value: (10000, 1),
                term: Some(78)
            }
        );
    }

    fn add_terms() -> Vec<LogEntry> {
        let mut vec: Vec<LogEntry> = Vec::new();
        for i in 0..100 {
            vec.push(LogEntry {
                value: (i, i),
                term: Some(i as usize),
            })
        }
        vec
    }

    #[test]
    fn test_oversize_index() {
        let mut log: Log = Log::new();
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(1),
                    value: (2, 2)
                }],
                Some(6),
                Some(100)
            ),
            false
        );
    }

    #[test]
    fn test_lots_of_entries() {
        let mut log: Log = Log::new();

        let mut vec: Vec<LogEntry> = Vec::new();
        for i in 0..100 {
            vec.push(LogEntry {
                value: (i, i),
                term: Some(i as usize),
            })
        }
        log.append_entries(&mut vec, Some(1), None);

        for i in 0..100 {
            assert_eq!(log.log[i].term, Some(i));
            assert_eq!(log.log[i].value, (i as i32, i as i32));
        }

        assert_eq!(log.log.len(), 100);
    }

    #[test]
    fn it_works() {
        let mut log: Log = Log::new();
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (1, 1)
                }],
                Some(0),
                None
            ),
            true
        );
        assert_eq!(log.log[0].value, (1, 1));
        assert_eq!(log.log[0].term, Some(0));

        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(1),
                    value: (2, 2)
                }],
                Some(0),
                Some(0)
            ),
            true
        );
        assert_eq!(log.log[1].value, (2, 2));
        assert_eq!(log.log[1].term, Some(1));

        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(3),
                    value: (3, 3)
                }],
                Some(1),
                Some(1)
            ),
            true
        );
        assert_eq!(log.log.len(), 3);
        assert_eq!(log.log[2].value, (3, 3));
        assert_eq!(log.log[2].term, Some(3));
    }

    #[test]
    fn test_idempotence() {
        let mut log: Log = Log::new();
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (1, 1)
                }],
                Some(0),
                None
            ),
            true
        );
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (2, 2)
                }],
                Some(0),
                None
            ),
            true
        );
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (3, 3)
                }],
                Some(0),
                None
            ),
            true
        );
        assert_eq!(log.log.len(), 1);
        assert_eq!(log.log[0].value, (3, 3));
        assert_eq!(log.log[0].term, Some(0));
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (4, 4)
                }],
                Some(0),
                Some(0)
            ),
            true
        );
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(1),
                    value: (5, 5)
                }],
                Some(0),
                Some(1)
            ),
            true
        );
        assert_eq!(log.log.len(), 3);
        assert_eq!(log.log[2].value, (5, 5));
        assert_eq!(log.log[2].term, Some(1));
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (6, 6)
                }],
                Some(0),
                Some(0)
            ),
            true
        );
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (6, 6)
                }],
                Some(0),
                Some(0)
            ),
            true
        );
        assert_eq!(log.log.len(), 3);
        assert_eq!(log.log[1].value, (6, 6));
        assert_eq!(log.log[1].term, Some(0));
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (6, 6)
                }],
                Some(0),
                None
            ),
            true
        );
        assert_eq!(
            log.append_entries(
                &mut vec![LogEntry {
                    term: Some(0),
                    value: (6, 6)
                }],
                Some(0),
                None
            ),
            true
        );
        assert_eq!(log.log.len(), 3);
        assert_eq!(log.log[0].value, (6, 6));
        assert_eq!(log.log[0].term, Some(0));
    }
}
