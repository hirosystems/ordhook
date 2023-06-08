FROM rust:bullseye as build

WORKDIR /src

RUN apt update && apt install -y ca-certificates pkg-config libssl-dev libclang-11-dev

RUN rustup update 1.67.0 && rustup default 1.67.0

COPY ./components/chainhook-cli /src/components/chainhook-cli

COPY ./components/chainhook-types-rs /src/components/chainhook-types-rs

COPY ./components/chainhook-sdk /src/components/chainhook-sdk

WORKDIR /src/components/chainhook-cli

RUN mkdir /out

RUN cargo build --features release --release

RUN cp target/release/chainhook /out

FROM debian:bullseye-slim

RUN apt update && apt install -y ca-certificates libssl-dev

COPY --from=build /out/ /bin/

WORKDIR /workspace

ENTRYPOINT ["chainhook"]