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

### Internal vs. external OAuth provider
The VTN implementation does feature an implementation of an OAuth provider including user management APIs
to allow for an easy setup.
The OpenADR specification does not require this feature but mentions that there must exist some OAuth provider somewhere.
Generally, the idea of OAuth is to decouple the authorization from the resource server, here the VTN.
Therefore, the OAuth provider feature is optional.
You can either disable it during compile time or runtime.

**During runtime**
The OAuth configuration of the VTN is done via the following environment variables:
- `OAUTH_TYPE` (allowed values: `INTERNAL`, `EXTERNAL`. Defaults to `INTERNAL`)
- `OAUTH_BASE64_SECRET` (must be at least 256 bit long. Required if `OAUTH_KEY_TYPE` is `HMAC`)
- `OAUTH_KEY_TYPE`(allows values: `HMAC`, `RSA`, `EC`, `ED`. Defaults to `HMAC`)
- `OAUTH_JWKS_LOCATION` (path to the OAUTH server well known JWKS endpoint. Required for all `OAUTH_KEY_TYPE`s, except `HMAC`)
- `OAUTH_VALID_AUDIENCES` (specifies the list of valid audiences for token validation, ensuring that the token is intended for the correct recipient. Required when `OAUTH_TYPE` is `EXTERNAL`. Optional and defaults to an empty list when `OAUTH_TYPE` is `INTERNAL`, which will fail validation if an `aud` claim is present in the decoded access token.)

The internal OAuth provider does only support `HMAC`.

**During compiletime**
If you already know that you don't need the internal OAuth feature,
you can disable it during compilation with the feature flag `internal-oauth`, which is enabled by default.
Therefore, run
```bash
cargo build/run --bin openleadr-vtn --no-default-features --features=postgres [--release]
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
