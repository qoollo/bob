# build image
FROM rust:1.47.0 as cargo-build

# rust toolchain version
ARG rust_tc_ver=nightly-2020-10-17
# size of storage key
ARG key_size=8

RUN apt-get update \
  && apt-get install -y --no-install-recommends musl-tools \
  && rustup install $rust_tc_ver \
  && rustup default $rust_tc_ver \
  && rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/bob

# crates downloading and initial build
COPY Cargo.toml Cargo.toml
RUN mkdir target && mkdir src/bin -p
ENV OUT_DIR /usr/src/bob/target

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobd.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobc.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobp.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/ccg.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/dcr.rs \
  && cargo build --release --target=x86_64-unknown-linux-musl

# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {tonic::include_proto!(\"bob_storage\");}" > src/lib.rs \
  && mkdir proto
COPY proto/* proto/
COPY build.rs .
RUN cargo build --release --target=x86_64-unknown-linux-musl \
  && rm -f target/x86_64-unknown-linux-musl/release/deps/bob*

# final build
COPY . .
ENV KEY_SIZE=${key_size}
RUN cargo build --release --target=x86_64-unknown-linux-musl

# bobd image
FROM alpine:3.12.0

# SSH
ENV NOTVISIBLE "in users profile"
RUN apk update \
  && apk add --no-cache openssh-server openssh-client sudo rsync \
  && mkdir /var/run/sshd \
  && echo 'root:bob' | chpasswd \
  && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
  && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
  && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/init.d/sshd \
  && echo "export VISIBLE=now" >> /etc/profile \
  && addgroup -g 1000 -S bobd \
  && adduser --shell /bin/sh -G bobd -S bobd

WORKDIR /home/bob/bin/
COPY --from=cargo-build /usr/src/bob/target/x86_64-unknown-linux-musl/release/bobd .
RUN chown bobd:bobd bobd \
  && mkdir ~/.ssh \
  && chmod 600 -R ~/.ssh \
  && echo -e "\
    cp /local_ssh/* ~/.ssh\n\
    chown -R root ~/.ssh\n\
    eval $(ssh-agent)\n\
    ssh-add ~/.ssh/id_rsa \n\
    /usr/sbin/sshd -D & \n\
    su -c \"./bobd -c /configs/\$1 -n /configs/\$2\" bobd" >> run.sh \
  && chmod +x run.sh

EXPOSE 80
EXPOSE 22
EXPOSE 20000

ENTRYPOINT ["/bin/sh", "./run.sh"]
