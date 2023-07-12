FROM --platform=amd64 ghcr.io/cross-rs/aarch64-unknown-linux-musl:latest AS build

RUN apt update && apt install -y git curl unzip musl-dev musl-tools

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh && sh rustup.sh -y
RUN . /root/.cargo/env && rustup target add aarch64-unknown-linux-musl

WORKDIR /
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-linux-x86_64.zip
RUN unzip -d /usr/local protoc-21.12-linux-x86_64.zip

WORKDIR /rmb-rs
COPY . .
RUN . /root/.cargo/env && cargo build --release --target aarch64-unknown-linux-musl
RUN aarch64-linux-musl-strip /rmb-rs/target/aarch64-unknown-linux-musl/release/rmb-peer

FROM --platform=arm64 alpine
COPY --from=build /rmb-rs/target/aarch64-unknown-linux-musl/release/rmb-peer /usr/sbin
ENTRYPOINT [ "/usr/sbin/rmb-peer" ]
