# build image
FROM rustlang/rust:nightly as cargo-build

RUN apt-get update

RUN apt-get install musl-tools -y

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/bob

# crates downloading and initial build
COPY Cargo.toml Cargo.toml
RUN mkdir target
ENV OUT_DIR /usr/src/bob/target
RUN mkdir src/
RUN mkdir src/bin

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/lib.rs
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobd.rs
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobc.rs
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobp.rs
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/ccg.rs
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/dcr.rs

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {tonic::include_proto!(\"bob_storage\");}" > src/lib.rs
RUN mkdir proto
COPY proto/* proto/
COPY build.rs .
RUN ls proto
RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

RUN rm -f target/x86_64-unknown-linux-musl/release/deps/bob*

# final build
COPY . .

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

# bobd image
FROM alpine:latest

RUN addgroup -g 1000 bobd

RUN adduser -D -s /bin/sh -u 1000 -G bobd bobd

WORKDIR /home/bob/bin/

COPY --from=cargo-build /usr/src/bob/target/x86_64-unknown-linux-musl/release/bobd .

RUN chown bobd:bobd bobd

USER bobd

CMD ["./bobd", "-c", "/configs/cluster.test.yaml", "-n", "/configs/node.test.yaml"]