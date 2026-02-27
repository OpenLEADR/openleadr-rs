FROM rust:1.93-alpine AS builder

ADD . /app
WORKDIR /app
COPY . .

# Don't depend on live sqlx during build use cached .sqlx
RUN SQLX_OFFLINE=true cargo build --release --bin openleadr-vtn --features internal-oauth
RUN cp /app/target/release/openleadr-vtn /app/openleadr-vtn

FROM alpine:latest AS final

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
