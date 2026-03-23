FROM rust:1.94-alpine AS builder

ADD . /app
WORKDIR /app
COPY . .

RUN apk add cmake g++ make openssl3-dev libgcc

# Don't depend on live sqlx during build use cached .sqlx
RUN --mount=type=cache,target=/app/target \
    SQLX_OFFLINE=true RUSTFLAGS="-Ctarget-feature=-crt-static" \
    cargo build --bin openleadr-vtn --features internal-oauth && \
    cp /app/target/debug/openleadr-vtn /app/openleadr-vtn

FROM alpine:latest AS final

RUN apk add libssl3 libgcc

# create a non root user to run the binary
ARG user=nonroot
ARG group=nonroot
ARG uid=2000
ARG gid=2000
RUN addgroup -g ${gid} ${group} && \
    adduser -u ${uid} -G ${group} -s /bin/sh -D ${user}

EXPOSE 3000

WORKDIR /dist

COPY --from=builder --chown=root:root --chmod=755 /app/openleadr-vtn/openleadr-vtn /dist/openleadr-vtn

USER $user

ENTRYPOINT ["/dist/openleadr-vtn"]
