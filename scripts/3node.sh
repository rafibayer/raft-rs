cargo build --release

@echo "Starting nodes"
sleep 0.5
cargo run --release --bin server -- -c config/local_3_nodes.json -n 0 &
sleep 0.5
cargo run --release --bin server -- -c config/local_3_nodes.json -n 1 &
sleep 0.5
cargo run --release --bin server -- -c config/local_3_nodes.json -n 2 &

