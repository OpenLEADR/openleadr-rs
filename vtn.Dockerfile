FROM rust:1.90-alpine AS base

# Install build dependencies
RUN apk add --no-cache alpine-sdk openssl-dev openssl-libs-static

FROM base AS builder

ADD . /app
WORKDIR /app
COPY . .

# Don't depend on live sqlx during build use cached .sqlx
RUN SQLX_OFFLINE=true cargo build --release --bin openleadr-vtn
RUN cp /app/target/release/openleadr-vtn /app/openleadr-vtn

FROM alpine:latest AS final

# Install OpenSSL
RUN apk add --no-cache openssl-libs-static curl

# create a non root user to run the binary
ARG user=nonroot
ARG group=nonroot
ARG uid=2000
ARG gid=2000
RUN addgroup -g ${gid} ${group} && \
    adduser -u ${uid} -G ${group} -s /bin/sh -D ${user}

EXPOSE 3000

WORKDIR /dist

# get the pre-built binary from builder so that we don't have to re-build every time
COPY --from=builder --chown=nonroot:nonroot /app/openleadr-vtn/openleadr-vtn /dist/openleadr-vtn
RUN chmod 777 /dist/openleadr-vtn

USER $user

ENTRYPOINT ["/dist/openleadr-vtn"]