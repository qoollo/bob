# build image
FROM rust:1.47.0 as cargo-build

# rust toolchain version
ARG RUST_TC_VER=stable
ARG KEY_SIZE=8
ARG COMMIT_HASH
ARG BUILD_PROFILE
ARG BUILD_PROFILE_DIR

RUN apt-get update \
  && apt-get install -y --no-install-recommends musl-tools \
  && rustup install $RUST_TC_VER \
  && rustup default $RUST_TC_VER \
  && rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/bob

# crates downloading and initial build
RUN mkdir -p bob/src bob-backend/src bob-common/src bob-grpc/src bob-apps/bin bob-access/src
RUN mkdir target
ENV OUT_DIR /usr/src/bob/target
COPY Cargo.toml Cargo.toml
COPY bob/Cargo.toml bob/Cargo.toml
COPY bob-backend/Cargo.toml bob-backend/Cargo.toml
COPY bob-common/Cargo.toml bob-common/Cargo.toml
COPY bob-grpc/Cargo.toml bob-grpc/Cargo.toml
COPY bob-apps/Cargo.toml bob-apps/Cargo.toml
COPY bob-access/Cargo.toml bob-access/Cargo.toml
RUN sed -i "s|\[\[bench\]\]|\[\[bench_ignore\]\]|g" */Cargo.toml

# estimate build directory
RUN export "BUILD_PROFILE_DIR=$(case "$BUILD_PROFILE" in\
  ("dev") echo "debug";;\
  ("test") echo "debug";;\
  ("bench") echo "release";;\
  (*) echo "$BUILD_PROFILE";;\
  esac)"

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > bob/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-backend/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-common/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-grpc/src/lib.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/bobd.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/bobc.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/bobp.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/ccg.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/dcr.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/brt.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-apps/bin/bobt.rs \
  && echo "fn main() {println!(\"if you see this, the build broke\")}" > bob-access/src/lib.rs \
  && cargo build --profile=${BUILD_PROFILE:-"release"} --target=x86_64-unknown-linux-musl


# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {include!(\"bob_storage.rs\");}" > bob-grpc/src/lib.rs \
  && mkdir -p bob-grpc/proto
COPY bob-grpc/proto/* bob-grpc/proto/
COPY bob-grpc/build.rs bob-grpc/build.rs
RUN cargo build --profile=${BUILD_PROFILE:-"release"} --target=x86_64-unknown-linux-musl \
  && rm -f target/x86_64-unknown-linux-musl/${BUILD_PROFILE_DIR:-"release"}/deps/bob* \
  && rm -f target/x86_64-unknown-linux-musl/${BUILD_PROFILE_DIR:-"release"}/deps/libbob* 

# final build
COPY . .
ENV BOB_KEY_SIZE=${KEY_SIZE}
ENV BOB_COMMIT_HASH=${COMMIT_HASH}
RUN cargo build --profile=${BUILD_PROFILE_DIR:-"release"} --target=x86_64-unknown-linux-musl

# bobd image
FROM alpine:3.12.0

# SSH
ENV NOTVISIBLE "in users profile"
RUN apk update \
  && apk add --no-cache openssh-server openssh-client sudo rsync bash \
  && mkdir /var/run/sshd \
  && echo 'root:bob' | chpasswd \
  && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
  && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
  && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/init.d/sshd \
  && echo "export VISIBLE=now" >> /etc/profile \
  && echo '%wheel ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/wheel \
  && addgroup -g 1000 -S bobd \
  && adduser --shell /bin/bash -G bobd -G wheel -S bobd

WORKDIR /home/bob/bin/
COPY --chown=bobd:bobd --from=cargo-build /usr/src/bob/target/x86_64-unknown-linux-musl/${BUILD_PROFILE_DIR:-"release"}/bobd .
RUN mkdir /bob/log -p && chown bobd:bobd /bob/log -R \
  && mkdir /bob/data/d1 -p && chown bobd:bobd /bob/data/d1 -R \
  && mkdir /bob/configs -p && chown bobd:bobd /bob/configs -R \
  && mkdir ~/.ssh \
  && chmod 600 -R ~/.ssh \
  && echo -e "#!/bin/bash\n\
  cp /local_ssh/* ~/.ssh\n\
  chown -R root ~/.ssh\n\
  eval $(ssh-agent)\n\
  ssh-add ~/.ssh/id_rsa\n\
  /usr/sbin/sshd -D &" >> prep.sh \
  && chmod +x prep.sh \
  && echo -e "#!/bin/bash\n\
  trap 'kill -TERM \$! && wait' SIGTERM\n\
  ./bobd -c /bob/configs/\$1 -n /bob/configs/\$2 \${@:3} &\n\
  wait" >> bobd.sh \
  && chmod +x bobd.sh \
  && echo -e "#!/bin/bash\n\
  trap 'kill -TERM \$! && wait' SIGTERM\n\
  sudo -S ./prep.sh\n\
  ./bobd.sh \$@ & \n\
  wait" >> run.sh \
  && chmod +x run.sh

COPY dockerfiles/default-configs/ /bob/configs

EXPOSE 22 80 20000
USER bobd
ENTRYPOINT ["/bin/bash", "./run.sh"]
CMD ["cluster.yaml", "node.yaml"]