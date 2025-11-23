FROM rust:1.78 as builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:stable-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/leaglag /usr/local/bin/leaglag
COPY config.json .
COPY simbols.json .
COPY blocked_symbols.json .

CMD ["leaglag"]
