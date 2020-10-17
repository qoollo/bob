default: test

build:
				cargo build --release

test: build
				cargo test

clean:
				rm -r target
