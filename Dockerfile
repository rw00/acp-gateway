FROM rust:1.86-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml ./
# Cache dependencies before copying full source when possible
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && cargo fetch
COPY src ./src
COPY tests ./tests
RUN cargo build --release

FROM builder AS test
RUN apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
CMD ["cargo", "test", "--", "--nocapture"]

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/acp-gateway /usr/local/bin/acp-gateway
ENV ACP_GATEWAY_HOST=0.0.0.0
ENV ACP_GATEWAY_PORT=8765
EXPOSE 8765
ENTRYPOINT ["acp-gateway"]
