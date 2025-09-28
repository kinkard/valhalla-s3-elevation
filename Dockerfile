FROM rust:alpine AS builder

# Dependencies for some crates if needed
RUN apk add --no-cache alpine-sdk

WORKDIR /usr/src/app

# First build a dummy target to cache dependencies in a separate Docker layer
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() { println!("Dummy image called!"); }' > src/main.rs
RUN cargo build --release

# Now build the real target
COPY src ./src
# Update modified attribute as otherwise cargo won't rebuild it
RUN touch -a -m ./src/main.rs
RUN cargo build --release

FROM alpine AS runtime
COPY --from=builder /usr/src/app/target/release/valhalla-s3-elevation /usr/local/bin/valhalla-s3-elevation
WORKDIR /elevation
ENTRYPOINT ["valhalla-s3-elevation"]
