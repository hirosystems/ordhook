FROM rust:bullseye as build

WORKDIR /src

RUN apt update && apt install -y ca-certificates pkg-config libssl-dev libclang-11-dev

RUN rustup update 1.67.0 && rustup default 1.67.0

COPY ./components/ordhook-cli /src/components/ordhook-cli

COPY ./components/ordhook-core /src/components/ordhook-core

WORKDIR /src/components/ordhook-cli

RUN mkdir /out

RUN cargo build --features release --release

RUN cp target/release/ordhook /out

FROM debian:bullseye-slim

RUN apt update && apt install -y ca-certificates libssl-dev

COPY --from=build /out/ordhook /bin/ordhook

WORKDIR /workspace

ENTRYPOINT ["ordhook"]
