use std::{sync::{Arc, Mutex}, collections::{HashMap, HashSet}};

use crate::{transport::MockTransport, node::Node};




#[test]
fn test_startup() {
    let n = 3;

    let transport = Arc::new(Mutex::new(MockTransport::new()));

    let mut nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        nodes.push(Arc::new(Mutex::new(Node::new(i, node_set.clone(), transport.clone(), HashMap::new()))));
    }

    let mut inboxes = HashMap::new();
    for i in 0..n {
        inboxes.insert(i, nodes[i].lock().unwrap().get_inbox());
        nodes[i].lock().unwrap().start(nodes[i].clone());
    }

    transport.lock().unwrap().setup_inboxes(inboxes);


    
}