# build image
FROM rust:1.47.0 as cargo-build

# rust toolchain version
ARG RUST_TC_VER=nightly-2021-06-23

RUN rustup install $RUST_TC_VER \
  && rustup default $RUST_TC_VER \
  && rustup target add x86_64-unknown-linux-gnu

WORKDIR /usr/src/bob

# crates downloading and initial build
RUN mkdir -p bob/src bob-backend/src bob-common/src bob-grpc/src bob-tools/bin
RUN mkdir target
ENV OUT_DIR /usr/src/bob/target
COPY Cargo.toml Cargo.toml
COPY bob/Cargo.toml bob/Cargo.toml
COPY bob-backend/Cargo.toml bob-backend/Cargo.toml
COPY bob-common/Cargo.toml bob-common/Cargo.toml
COPY bob-grpc/Cargo.toml bob-grpc/Cargo.toml
COPY bob-tools/Cargo.toml bob-tools/Cargo.toml

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > bob/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-backend/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-common/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-grpc/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-tools/bin/bobd.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-tools/bin/bobc.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-tools/bin/bobp.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-tools/bin/ccg.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-tools/bin/dcr.rs \
  && cargo build --release --target=x86_64-unknown-linux-gnu


# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {include!(\"bob_storage.rs\");}" > bob-grpc/src/lib.rs \
  && mkdir -p bob-grpc/proto
COPY bob-grpc/proto/* bob-grpc/proto/
COPY bob-grpc/build.rs bob-grpc/build.rs
RUN cargo build --release --target=x86_64-unknown-linux-gnu \
  && rm -f target/x86_64-unknown-linux-musl/release/deps/bob*

# final build
COPY . .
RUN cargo build --release --target=x86_64-unknown-linux-gnu

# bobd image
FROM ubuntu:20.10

# SSH
ENV NOTVISIBLE "in users profile"
RUN apt-get update \
  && apt-get install -y --no-install-recommends openssh-server openssh-client sudo rsync \
  && mkdir /var/run/sshd \
  && echo 'root:bob' | chpasswd \
  && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
  && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
  && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \
  && echo "export VISIBLE=now" >> /etc/profile \
  && groupadd -g 1000 bobd \
  && useradd -s /bin/sh -u 1000 -g bobd bobd \
  && usermod -aG sudo bobd \
  && echo 'bobd:bobd' | chpasswd \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /home/bob/bin/
COPY --from=cargo-build /usr/src/bob/target/x86_64-unknown-linux-gnu/release/bobd .
RUN chown bobd:bobd bobd \
  && mkdir ~/.ssh \
  && chmod 600 -R ~/.ssh \
  && echo "#!/bin/bash\n\
    cp /local_ssh/* ~/.ssh\n\
    chown -R root ~/.ssh\n\
    eval $(ssh-agent)\n\
    ssh-add ~/.ssh/id_rsa\n\
    /usr/sbin/sshd -D &" >> prep.sh \
  && chmod +x prep.sh \
  && echo "#!/bin/bash\n\
    trap 'kill -TERM \$! && wait' SIGTERM\n\
    ./bobd -c /configs/\$1 -n /configs/\$2 &\n\
    wait" >> bobd.sh \
  && chmod +x bobd.sh \
  && echo "#!/bin/bash\n\
    trap 'kill -TERM \$! && wait' SIGTERM\n\
    echo 'bobd' | sudo -S ./prep.sh\n\
    ./bobd.sh \$1 \$2 & \n\
    wait" >> run.sh \
  && chmod +x run.sh

EXPOSE 80
EXPOSE 22
EXPOSE 20000
USER bobd
ENTRYPOINT ["./run.sh"]
