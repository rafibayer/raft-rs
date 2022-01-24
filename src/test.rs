use std::{collections::HashMap, thread, time::Duration};

use rand::Rng;

use crate::{node::Node, async_tcp, raft::CommandRequest};

#[test]
fn test_replicate() {
    let port = 7878;
    let n = 5;

    let mut cluster = HashMap::new();

    for i in 0..n {
        cluster.insert(i, format!("127.0.0.1:{}", port + i).parse().unwrap());
    }

    for i in 0..n {
        let cluster = cluster.clone();
        thread::spawn(move || {
            let mut node = Node::new(i, cluster, HashMap::new());
            node.start();
        });
    }

    // give time to reach quorum
    thread::sleep(Duration::from_secs(3));

    for i in 0..500 {
        async_tcp::apply_command(
            CommandRequest { command: format!("SET X {i}") },
            rand::thread_rng().gen_range(0..n),
            &cluster,
        )
        .unwrap();

        let result =
        async_tcp::apply_command(CommandRequest { command: "GET X".to_string() }, 0, &cluster)
            .unwrap();

        assert_eq!(i.to_string(), result);
    }
}
