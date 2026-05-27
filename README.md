[![Rust](https://github.com/threefoldtech/zos_rmb/actions/workflows/rust.yaml/badge.svg)](https://github.com/threefoldtech/zos_rmb/actions/workflows/rust.yaml)

# ZOS RMB - Reliable Message Bus

Reliable Message Bus is a secure peer-to-peer messaging protocol that enables services and nodes to communicate reliably, even when running behind NAT. It provides message authenticity guarantees, end-to-end encryption, and support for federated third-party relays.

## What this is

ZOS RMB is a messaging system designed for machine-to-machine communication. It allows bots (automated services) to send and receive messages in a chat-like pattern, with the following properties:

- **Message authenticity**: Messages are cryptographically signed so the receiver can verify the sender's identity.
- **End-to-end encryption**: Messages are encrypted so that relays cannot inspect content.
- **Federated relays**: Anyone can host a relay; users are protected by encryption even when using third-party infrastructure.

## What this repository contains

- **`rmb-peer`** — A gateway binary that connects to a relay on behalf of your identity, handling reconnection, verification, decryption, and exposing a simple Redis-based plain-text API
- **Relay server** — The message routing infrastructure that forwards messages between peers
- **Protocol libraries** — Rust crates implementing the ZOS RMB protocol (signing, verification, encryption, message formats)
- **Protocol specification** — Detailed protocol documentation in the [docs](docs/readme.md) directory

## Role in the stack

ZOS RMB serves as the messaging backbone for the infrastructure stack. Nodes, users, and services use it to exchange commands, status updates, and responses. Identities are maintained on a blockchain (each peer needs an account), and public encryption keys are published there for anyone to use. The `rmb-peer` gateway abstracts the cryptographic complexity, allowing clients to interact via simple Redis queue operations using plain JSON.

## Mycelium

Mycelium is the network layer used to provide secure, peer-to-peer connectivity between nodes, services, and users. It enables decentralized networking across the infrastructure stack and is used as part of the ThreeFold Grid deployment.

## ZOS / Zero-OS

ZOS, also known as Zero-OS, is the operating system layer used to run and manage nodes. It provides the low-level runtime environment for workloads, networking, storage, and automation. ZOS RMB is the primary communication channel used by ZOS nodes to receive commands and report status.

## Relation to ThreeFold

This technology is used within the ThreeFold ecosystem and was first deployed on the ThreeFold Grid. The component itself is designed as reusable infrastructure technology and should be understood by its technical function first, independent of any specific deployment.

## Ownership

This repository is owned and maintained by TF-Tech NV, a Belgian company responsible for the development and maintenance of this technology.

## Specification

For details about the protocol itself, see the [docs](docs/readme.md).

## How to use

ZOS RMB is built for bots and software to communicate, not for human chat. There is no mobile app; instead, libraries are provided for connecting to the network and exchanging messages with other bots.

### Using an existing library

If a library exists for your language, follow its documentation to implement a service bot or make requests to other bots.

#### Known libraries

- Go: [rmb-sdk-go](https://github.com/threefoldtech/zos_sdk_go/tree/development/rmb-sdk-go)
- TypeScript: [rmb-sdk-ts](https://github.com/threefoldtech/zos_sdk_ts)

### Using `rmb-peer`

If no library is available for your language, you can use `rmb-peer` as a gateway. `rmb-peer` uses your mnemonic (identity secret key) to assume your identity and connects to the relay on your behalf. It maintains the connection indefinitely and handles:

- Reconnecting if the connection is lost
- Verifying received messages
- Decrypting received messages
- Sending requests on your behalf, handling all cryptographic operations

It exposes a simple plain-text API over Redis. To send messages or handle requests, you only need to push and pop messages from Redis queues. Messages are simple plain-text JSON.

> More details about the structure of the messages are also in the [docs](docs/readme.md).

## Download

Please check the latest [releases](https://github.com/threefoldtech/zos_rmb/releases). Normally you only need the `rmb-peer` binary, unless you want to host your own relay.

## Build from source

### Prerequisites

- Download Rustup and install Rust:

  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

- Install the standard library for the target `x86_64-unknown-linux-musl`:

  ```bash
  rustup target add x86_64-unknown-linux-musl
  ```

- Install a linker that supports musl, such as `musl-gcc`:

  ```bash
  sudo apt update
  sudo apt install musl-tools
  ```

- Install a pre-compiled `protoc` binary:

  ```bash
  PB_REL="https://github.com/protocolbuffers/protobuf/releases"
  curl -LO $PB_REL/download/v23.4/protoc-23.4-linux-x86_64.zip
  sudo unzip -d /usr/local protoc-23.4-linux-x86_64.zip
  protoc --version  # Ensure compiler version is updated
  ```

- Redis server:

  ```bash
  sudo apt update
  sudo apt install redis-server
  sudo systemctl enable redis-server
  sudo systemctl start redis-server
  ```

### Building

```bash
git clone git@github.com:threefoldtech/rmb-rs.git
cd zos_rmb
cargo build --release --target=x86_64-unknown-linux-musl
```

### Troubleshooting

- If you encounter an error like:

  ```
  types.proto: This file contains proto3 optional fields, but --experimental_allow_proto3_optional was not set.
  codegen failed: parse and typecheck
  ```

  **Solution**: Install `protoc` from pre-compiled binaries rather than your package manager. See prerequisites above.

- A peer must use a unique mnemonic or keys. Multiple peers using the same mnemonic will make message routing impossible. The same peer may make multiple connections to the same relay using different session IDs. A single peer may also connect to multiple relays for redundancy.

> RUNNING MULTIPLE PEERS WITH THE SAME MNEMONIC MUST BE AVOIDED UNLESS FOLLOWING THE GUIDELINES ABOVE

### Running tests

```bash
cargo test
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
