/// PORT START: 5000

use simple_logger::SimpleLogger;

use super::*;



/// test that we can shut down a node
#[test]
fn test_shutdown() {

    
    let (client, mut cluster) = create_local_cluster(1, 5000);

    client.admin(0, AdminRequest::Shutdown).unwrap();

    let done = Arc::new(Mutex::new(false));
    let done_clone = done.clone();
    
    thread::spawn(move || {
        cluster.remove(0).join().unwrap();
        *done_clone.lock().unwrap() = true;
    });

    // if we haven't shut down completely in 3 seconds, we will fail
    thread::sleep(Duration::from_secs(3));

    // assert that the node thread has stopped
    assert!(*done.lock().unwrap());
}

#[test]
fn test_become_leader() {
    let (client, cluster) = create_local_cluster(3, 5050);

    thread::sleep(Duration::from_secs(3));

    for i in 0..3 {
        client.admin(i, AdminRequest::BecomeLeader).unwrap();
        thread::sleep(Duration::from_millis(500)); // allow time to notify other nodes
        for j in 0..3 {
            let resp = client.admin(j, AdminRequest::GetLeader).unwrap();
            assert!(matches!(resp, AdminResponse::Leader(Some(i))));
        }
    }
}