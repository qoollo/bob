# build image
FROM rust:latest as cargo-build

RUN apt-get update \
  && rustup default nightly \
  && rustup target add x86_64-unknown-linux-gnu

WORKDIR /usr/src/bob

# crates downloading and initial build
COPY Cargo.toml Cargo.toml
RUN mkdir target
ENV OUT_DIR /usr/src/bob/target
RUN mkdir src/bin -p

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobd.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobc.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/bobp.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/ccg.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/dcr.rs \
  && cargo build --release --target=x86_64-unknown-linux-gnu

# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {tonic::include_proto!(\"bob_storage\");}" > src/lib.rs \
  && mkdir proto
COPY proto/* proto/
COPY build.rs .
RUN cargo build --release --target=x86_64-unknown-linux-gnu \
  && rm -f target/x86_64-unknown-linux-gnu/release/deps/bob*

# final build
COPY . .
RUN cargo build --release --target=x86_64-unknown-linux-gnu

# bobd image
FROM ubuntu:latest

# SSH
ENV NOTVISIBLE "in users profile"
RUN apt-get update && apt-get install -y openssh-server openssh-client sudo rsync \
  && mkdir /var/run/sshd \
  && echo 'root:bob' | chpasswd \
  && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
  && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
  && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \
  && echo "export VISIBLE=now" >> /etc/profile \
  && groupadd -g 1000 bobd \
  && useradd -s /bin/sh -u 1000 -g bobd bobd \
  && mkdir ~/.ssh \
  && chmod 600 -R ~/.ssh

WORKDIR /home/bob/bin/
COPY --from=cargo-build /usr/src/bob/target/x86_64-unknown-linux-gnu/release/bobd .
RUN chown bobd:bobd bobd \
  && echo "#!/bin/bash \n\
  cp ~/local_ssh/* ~/.ssh \
  chown -R root ~/.ssh \
  eval $(ssh-agent) \
  ssh-add ~/.ssh/id_rsa \
  /usr/sbin/sshd -D &\n\
  su -c \"./bobd -c \$CONFIG_DIR/cluster.yaml -n \$CONFIG_DIR/\$NODE_NAME.yaml\" bobd" >> run.sh \
  && chmod +x run.sh

ENTRYPOINT ["./run.sh"]


