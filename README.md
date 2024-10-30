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
docker compose up -d db
```

Run the [migrations](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md):

```bash
cargo sqlx migrate run
```

## How to use

Running the VTN using cargo:

```bash
RUST_LOG=trace cargo run --bin openleadr-vtn
```

Running the VTN using docker-compose:

```bash
docker compose up -d
```

### Note on prepared SQL

This workspace uses SQLX macro to type check SQL statements.
In order to build the crate without a running SQL server (such as in the docker), SQLX must be run in offline mode.
In this mode type checking is done via a cached variant of the DB (the .sqlx directory).
For this to work as intended, each time a change is made to SQL schemas or queries, please run

```bash
cargo sqlx prepare --workspace
```

This will update the cached SQL in the `.sqlx` directory which should be committed to GitHub.

### Invalidating the docker build cache

To expedite the slow cargo release builds, the Dockerfile uses a multi-stage build.
If changes have been made and are not being reflected in the binary running inside docker, try

```bash
docker compose up --force-recreate --build --no-deps vtn
```
This will force a rebuild


Running the client

```bash
cargo run --bin openleadr
```
