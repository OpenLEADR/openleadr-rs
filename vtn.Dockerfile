FROM rust:1.81 as base
RUN apt-get update && apt-get install -y --no-install-recommends curl && apt-get clean

FROM base as builder

ADD . /app
WORKDIR /app
COPY . .

# Don't depend on live sqlx during build use cached .sqlx
RUN SQLX_OFFLINE=true cargo build --release --bin openleadr-vtn
RUN cp /app/target/release/openleadr-vtn /app/openleadr-vtn

FROM debian:bookworm-slim as final
RUN apt-get update && apt-get install curl -y

# create a non root user to run the binary
ARG user=nonroot
ARG group=nonroot
ARG uid=2000
ARG gid=2000
RUN addgroup --gid ${gid} ${group} && adduser --uid ${uid} --gid ${gid} --system --disabled-login --disabled-password ${user}
EXPOSE 3000
# get the pre-built binary from builder so that we don't have to re-build every time
COPY --from=1 --chown=nonroot:nonroot /app/openleadr-vtn/openleadr-vtn /home/nonroot/openleadr-vtn
RUN chmod 777 /home/nonroot/openleadr-vtn

USER $user

ENTRYPOINT ["./home/nonroot/openleadr-vtn"]
