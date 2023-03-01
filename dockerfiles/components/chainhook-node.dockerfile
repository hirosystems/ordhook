FROM rust:bullseye as build

WORKDIR /src

RUN apt update && apt install -y ca-certificates pkg-config libssl-dev

RUN rustup update 1.59.0 && rustup default 1.59.0

COPY ./components/chainhook-cli /src/components/chainhook-cli

COPY ./components/chainhook-types-rs /src/components/chainhook-types-rs

COPY ./components/chainhook-event-observer /src/components/chainhook-event-observer

WORKDIR /src/components/chainhook-cli

RUN mkdir /out

RUN cargo build --features release --release

RUN cp target/release/chainhook-cli /out

FROM debian:bullseye-slim

RUN apt update && apt install -y libssl-dev

COPY --from=build /out/ /bin/

WORKDIR /workspace

ENTRYPOINT ["chainhook-cli"]