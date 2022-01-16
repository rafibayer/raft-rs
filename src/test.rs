use std::{sync::{Arc}, collections::{HashMap, HashSet}};
use parking_lot::Mutex;

use simple_logger::SimpleLogger;

use crate::{transport::MockTransport, node::Node};




#[test]
fn test_startup() {
    SimpleLogger::new().init().unwrap();
    let n = 3;

    let mut transport = MockTransport::new();

    let mut nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        nodes.push(Arc::new(Mutex::new(Node::new(i, node_set.clone(), HashMap::new()))));
    }

    let mut inboxes = HashMap::new();
    for i in 0..n {
        inboxes.insert(i, nodes[i].lock().get_inbox());
    }

    transport.setup_inboxes(inboxes);
    let transport_arc = Arc::new(transport);

    for i in 0..n {
        nodes[i].lock().start(nodes[i].clone(), transport_arc.clone());
    }
}