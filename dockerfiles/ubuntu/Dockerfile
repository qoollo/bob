# build image
FROM rust:1.66.0 as cargo-build

# rust toolchain version
ARG RUST_TC_VER=stable
ARG KEY_SIZE=8
ARG COMMIT_HASH
ARG BUILD_PROFILE=release
ARG BUILD_TARGET=x86_64-unknown-linux-gnu

RUN rustup install $RUST_TC_VER \
  && rustup default $RUST_TC_VER \
  && rustup target add $BUILD_TARGET

WORKDIR /usr/src/bob

# crates downloading and initial build
RUN mkdir -p bob/src bob-backend/src bob-common/src bob-grpc/src bob-apps/bin bob-access/src
RUN mkdir target
COPY Cargo.toml Cargo.toml
COPY bob/Cargo.toml bob/Cargo.toml
COPY bob-backend/Cargo.toml bob-backend/Cargo.toml
COPY bob-common/Cargo.toml bob-common/Cargo.toml
COPY bob-grpc/Cargo.toml bob-grpc/Cargo.toml
COPY bob-apps/Cargo.toml bob-apps/Cargo.toml
COPY bob-access/Cargo.toml bob-access/Cargo.toml
RUN sed -i "s|\[\[bench\]\]|\[\[bench_ignore\]\]|g" */Cargo.toml

# estimate build directory
RUN echo "$(case "$BUILD_PROFILE" in\
  ("dev") echo "debug";;\
  ("test") echo "debug";;\
  ("bench") echo "release";;\
  (*) echo "$BUILD_PROFILE";;\
  esac)" >> ./build_profile_dir

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
  && cargo build --profile=${BUILD_PROFILE} --target=$BUILD_TARGET


# separate stage for proto build
RUN echo "fn main() {println!(\"if you see this, the build broke\")} pub mod grpc {include!(\"bob_storage.rs\");}" > bob-grpc/src/lib.rs \
  && mkdir -p bob-grpc/proto
COPY bob-grpc/proto/* bob-grpc/proto/
COPY bob-grpc/build.rs bob-grpc/build.rs
RUN cargo build --profile=${BUILD_PROFILE} --target=$BUILD_TARGET \
  && rm -f target/$BUILD_TARGET/$(cat ./build_profile_dir)/deps/bob* \
  && rm -f target/$BUILD_TARGET/$(cat ./build_profile_dir)/deps/libbob*

# final build
COPY . .
ENV BOB_KEY_SIZE=${KEY_SIZE}
ENV BOB_COMMIT_HASH=${COMMIT_HASH}
RUN cargo build --profile=${BUILD_PROFILE} --target=$BUILD_TARGET \
  && mkdir target/build_output \ 
  && mv target/$BUILD_TARGET/$(cat ./build_profile_dir)/bobd target/build_output/bobd

# bobd image
FROM ubuntu:20.04

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
COPY --chown=bobd:bobd --from=cargo-build /usr/src/bob/target/build_output/bobd .
RUN mkdir /bob/log -p && chown bobd:bobd /bob/log -R \
  && mkdir /bob/data/d1 -p && chown bobd:bobd /bob/data/d1 -R \
  && mkdir /bob/configs -p && chown bobd:bobd /bob/configs -R \
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
  ./bobd -c /bob/configs/\$1 -n /bob/configs/\$2 \${@:3} &\n\
  wait" >> bobd.sh \
  && chmod +x bobd.sh \
  && echo "#!/bin/bash\n\
  trap 'kill -TERM \$! && wait' SIGTERM\n\
  echo 'bobd' | sudo -S ./prep.sh\n\
  ./bobd.sh \$@ & \n\
  wait" >> run.sh \
  && chmod +x run.sh

COPY dockerfiles/default-configs/ /bob/configs

EXPOSE 22 80 20000
USER bobd
ENTRYPOINT ["./run.sh"]
CMD ["cluster.yaml", "node.yaml"]
