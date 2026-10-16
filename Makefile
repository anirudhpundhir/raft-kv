SEEDS ?= 1000

.PHONY: build test sim-soak lint fmt cluster bench clean

build:
	cargo build --release

test:          ## Unit, simulation (default seeds), Figure 8 and 3-process cluster tests
	cargo test --release

sim-soak:      ## Simulation with $(SEEDS) seeds per scenario
	RAFT_SIM_SEEDS=$(SEEDS) cargo test --release --test simulation

lint:
	cargo fmt --check
	cargo clippy --all-targets --release -- -D warnings

fmt:
	cargo fmt

CLUSTER = 1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002,3=127.0.0.1:7003/127.0.0.1:8003
cluster: build ## Run a local 3-node cluster in the foreground (Ctrl-C stops all)
	trap 'kill 0' INT; \
	for i in 1 2 3; do ./target/release/raft-kv --id $$i --cluster $(CLUSTER) --data data/$$i & done; wait

bench: build   ## Load-generate against a running `make cluster` (LEADER=127.0.0.1:800N)
	cargo run --release --example loadgen -- $(LEADER) 1 2000
	cargo run --release --example loadgen -- $(LEADER) 16 500
	cargo run --release --example loadgen -- $(LEADER) 64 500

clean:
	cargo clean; rm -rf data
