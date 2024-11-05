![maintenance-status](https://img.shields.io/badge/maintenance-actively--developed-brightgreen.svg)
![codecov](https://codecov.io/gh/OpenLEADR/openleadr-rs/graph/badge.svg?token=BKQ0QW9G8H)
![Checks](https://github.com/OpenLEADR/openleadr-rs/actions/workflows/checks.yml/badge.svg?branch=main)
![Crates.io Version](https://img.shields.io/crates/v/openleadr-vtn)

# OpenADR 3.0 VTN server in Rust

![LF energy OpenLEADR logo](../openleadr-logo.svg)

This crate contains an OpenADR VTN implementation.  

The following contains information specific to the VTN application, i.e., the server.
If you are interested in information about the whole project, please visit the [project level Readme](../README.md).

## Getting started
Your machine needs a recent version of Rust installed.
Please refer to the [official installation website](https://rustup.rs/) for the setup.
To apply the Database migrations, you also need the sqlx-cli installed.
Simply run `cargo install sqlx-cli`.

All the following commands are executed in the root directory of the Git repository.

### Database setup

First, start up a postgres database. For example, using docker compose:

```bash
docker compose up -d db
```

Run the [migrations](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md):

```bash
cargo sqlx migrate run
```

### How to use

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
