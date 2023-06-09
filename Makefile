default: build

build:
				cargo build --release

test: build
				cargo test --features testing

clean:
				rm -r target
				rm Cargo.lock
