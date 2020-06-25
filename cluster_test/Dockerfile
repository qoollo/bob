# build image
FROM rust:latest as cargo-build

RUN apt-get update

RUN rustup default nightly

RUN rustup target add x86_64-unknown-linux-gnu

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

RUN cargo build --release --target=x86_64-unknown-linux-gnu

# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {tonic::include_proto!(\"bob_storage\");}" > src/lib.rs
RUN mkdir proto
COPY proto/* proto/
COPY build.rs .
RUN cargo build --release --target=x86_64-unknown-linux-gnu

RUN rm -f target/x86_64-unknown-linux-gnu/release/deps/bob*

# final build
COPY . .

RUN cargo build --release --target=x86_64-unknown-linux-gnu

# bobd image
FROM ubuntu:latest

RUN groupadd -g 1000 bobd

RUN useradd -s /bin/sh -u 1000 -g bobd bobd

WORKDIR /home/bob/bin/

COPY --from=cargo-build /usr/src/bob/target/x86_64-unknown-linux-gnu/release/bobd .

RUN chown bobd:bobd bobd

USER bobd