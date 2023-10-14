FROM rust:bullseye as build

WORKDIR /src

RUN apt update && apt install -y ca-certificates pkg-config libssl-dev libclang-11-dev curl gnupg

RUN rustup update 1.72.0 && rustup default 1.72.0

RUN mkdir /out

ENV NODE_MAJOR=18

RUN mkdir -p /etc/apt/keyrings

RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update

RUN apt-get install nodejs -y

RUN npm install -g @napi-rs/cli yarn

COPY ./components/ordhook-core /src/components/ordhook-core

COPY ./components/ordhook-sdk-js /src/components/ordhook-sdk-js

COPY ./components/ordhook-cli /src/components/ordhook-cli

WORKDIR /src/components/ordhook-sdk-js

RUN yarn build

RUN cp target/release/ordhook /out

WORKDIR /src/components/ordhook-cli

RUN cargo build --features release --release

RUN cp target/release/ordhook /out

FROM debian:bullseye-slim

RUN apt update && apt install -y ca-certificates libssl-dev

COPY --from=build /out/ordhook /bin/ordhook

COPY --from=build /out/ordhook /bin/ordhook

WORKDIR /workspace

ENTRYPOINT ["ordhook"]
