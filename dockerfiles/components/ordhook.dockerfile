FROM rust:bullseye

WORKDIR /src

RUN apt-get update && apt-get install -y ca-certificates pkg-config libssl-dev libclang-11-dev curl gnupg libunwind-dev libunwind8

RUN rustup update 1.72.0 && rustup default 1.72.0

RUN mkdir /out

ENV NODE_MAJOR=18

RUN mkdir -p /etc/apt/keyrings

RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update

RUN apt-get install nodejs -y

RUN npm install -g @napi-rs/cli yarn

COPY ./Cargo.toml /src/Cargo.toml

COPY ./Cargo.lock /src/Cargo.lock

COPY ./components/ordhook-core /src/components/ordhook-core

COPY ./components/ordhook-sdk-js /src/components/ordhook-sdk-js

COPY ./components/ordhook-cli /src/components/ordhook-cli

WORKDIR /src/components/ordhook-sdk-js

# RUN yarn install

# RUN yarn build

# RUN cp *.node /out

WORKDIR /src/components/ordhook-cli

RUN cargo build --features release --release

RUN cp /src/target/release/ordhook /bin

RUN rm -rf  /src/target/

WORKDIR /workspace

ENTRYPOINT ["ordhook"]

# FROM debian:bullseye-slim

# WORKDIR /ordhook-sdk-js

# RUN apt-get update && apt-get install -y ca-certificates libssl-dev sqlite3 libunwind-dev libunwind8

# # COPY --from=build /out/*.node /ordhook-sdk-js/

# COPY --from=build /bin/ordhook /bin/ordhook

# WORKDIR /workspace

# ENTRYPOINT ["ordhook"]
