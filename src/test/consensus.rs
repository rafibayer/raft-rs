
use crate::raft::CommandResponse;

/// PORT START: 1000

use super::*;

#[test]
fn test_consensus() {
    let (mut client, cluster) = create_local_cluster(5, 1000);

    thread::sleep(Duration::from_secs(3));

    for i in 0..100 {
        client.apply_command(CommandRequest { command: format!("SET X {i}") })
            .unwrap();

        let resp = client.apply_command(CommandRequest { command: format!("GET X") })
            .unwrap();

        match resp {
            CommandResponse::Result(res) => assert_eq!(res, i.to_string()),
            _ => panic!("Unexpected command response type: {resp:?}")
        }
    }
}