# OpenADR 3.0 in Rust

This is a work-in-progress implementation of the OpenADR 3.0 specification.
OpenADR is a protocol for automatic demand-response in electricity grids, like dynamic pricing or load shedding.

## Limitations

This repository contains only OpenADR 3.0, older versions are not supported.
Currently, only the `/programs`, `/reports`, `/events` endpoints are supported.
Also no authentication is supported yet.

## Database setup

Startup a postgres database. For example, using docker compose:

```bash
docker compose up db
```

Run the [migrations](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md):

```bash
cargo sqlx migrate run
```

## How to use

Running the VTN using cargo:

```bash
RUST_LOG=trace cargo run --bin vtn
```

Running the VTN using docker-compose:

```bash
docker compose up
```

Running the client

```bash
cargo run --bin openadr
```
