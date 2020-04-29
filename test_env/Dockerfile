FROM ubuntu:latest
WORKDIR /home/py/rust/bob/
RUN apt update && apt install -y iproute2 iputils-ping netcat
# COPY ../target/debug/bobd /bin/bobd