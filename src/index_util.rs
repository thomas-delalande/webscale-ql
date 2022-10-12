use std::cmp::Ordering;

use crate::INT_SIZE_BITS;

#[derive(Clone, Eq, Debug)]
pub struct KeyValuePair {
    pub key: String,
    pub value: usize,
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for KeyValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for KeyValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl KeyValuePair {
    pub fn new(key: String, value: usize) -> KeyValuePair {
        KeyValuePair { key, value }
    }
}

#[derive(Debug)]
struct NodeData {
    value: String,
    position: usize,
}

fn read_node_from_bytes(bytes: &[u8], column_size: usize) -> NodeData {
    let value_bytes = &bytes[0..column_size];
    let position_bytes = &bytes[column_size..(column_size + INT_SIZE_BITS as usize)];
    let position = i64::from_str_radix(
        &std::str::from_utf8(position_bytes).unwrap().to_string(),
        16,
    )
    .unwrap();

    let value = std::str::from_utf8(value_bytes)
        .unwrap()
        .trim_start_matches(|x| x == '0');

    NodeData {
        value: value.to_string(),
        position: position.try_into().unwrap(),
    }
}
