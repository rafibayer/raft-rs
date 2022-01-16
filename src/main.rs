mod raft;
mod state;
mod node;
mod utils;
mod transport;

#[cfg(test)]
mod test;

use std::{thread, time::Duration, sync::{Arc}, collections::{HashSet, HashMap}};
use parking_lot::Mutex;

use simple_logger::SimpleLogger;

use node::Node;
use raft::{NetworkMessage, CommandRequest};
use transport::{MockTransport, Transport};


fn main() {
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

    transport_arc.send(0, NetworkMessage::CommandRequest(CommandRequest{command: "SET X 5".to_string()}));

    monitor_deadlocks();
    thread::sleep(Duration::from_secs(10000));
}

use parking_lot::deadlock;

fn monitor_deadlocks() {
    
    // Create a background thread which checks for deadlocks every 10s
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(500));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{}", i);
                for t in threads {
                    println!("Thread Id {:#?}", t.thread_id());
                    println!("{:#?}", t.backtrace());
                }
            }
        }
    });
}
