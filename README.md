![maintenance-status](https://img.shields.io/badge/maintenance-actively--developed-brightgreen.svg)
![codecov](https://codecov.io/gh/OpenLEADR/openleadr-rs/graph/badge.svg?token=BKQ0QW9G8H)
![Checks](https://github.com/OpenLEADR/openleadr-rs/actions/workflows/checks.yml/badge.svg?branch=main)

# OpenADR 3.0 in Rust

![LF energy OpenLEADR logo](./openleadr-logo.svg)

This repository contains an OpenADR 3.0 client (VEN) library and a server (VTN) implementation, both written in Rust.
OpenADR is a protocol for automated demand-response in electricity grids, like dynamic pricing or load shedding.
The [OpenADR alliance](https://www.openadr.org/) is responsible for the standard,
which can be [downloaded](https://www.openadr.org/specification) free of charge.
This implementation is still work-in-progress, and we aim for a first stable release in December 2024.

Thanks to our sponsors [Elaad](https://elaad.nl/en/) and [Tweede golf](https://tweedegolf.nl/en)
for making this work possible.

## Documentation

The documentation of the project is an ongoing effort as part of the first release.
The [`./openleadr-client`](./openleadr-client) and [`./openleadr-vtn`](./openleadr-vtn) contain Readmes on how to get
started with the client library and server, respectively.
Additionally, the [client](https://crates.io/crates/openleadr-client), [server](https://crates.io/crates/openleadr-vtn),
and [common data types](https://crates.io/crates/openleadr-wire) are published to crates.io
and have documentation available on docs.rs.
As an addition, [#17](https://github.com/OpenLEADR/openleadr-rs/issues/17) aims
to produce a detailed OpenAPI specification of the VTN API we provide. 

## Getting started

### First time setup

Your machine needs a recent version of Rust installed.
Please refer to the [official installation website](https://rustup.rs/) for instructions for your platform. To apply the database migrations, you also need the sqlx-cli installed.
Simply run `cargo install sqlx-cli`.

### Docker compose

For a quick start,
this repository contains a [`docker-compose.yml`](docker-compose.yml) with the VTN and a Postgres database.
To start it, first start the database and run the migrations:

```bash
docker compose up -d db # start the DB
cargo sqlx migrate run  # apply the migrations
docker compose up -d    # start all other containers, i.e., the VTN
```

Afterward, the VTN should be reachable at `http://localhost:3000`.

For a more detailed guide,
please refer to the Readmes in the [`./openleadr-client`](./openleadr-client) and 
[`./openleadr-vtn`](./openleadr-vtn) directories.

## Supported features

This repository contains only OpenADR 3.0, older versions are not supported.

Currently, real-time updates via the webhook mechanism, known as subscriptions in the specification, are not supported.
While we currently do not plan to add this ourselves, we warmly welcome any contribution or sponsoring to add it.
See the [Contributing section](#contributing) if you are interested.

At the moment, the VTN implements its own OAuth provider,
but we plan to allow for a third-party OAuth provider as well, 
see [#26](https://github.com/openLEADR/openleadr-rs/issues/26).

The client and server do support creating, retrieving, updating,
and deleting programs, events, reports, VENs, and resources.
Both sides support authentication and authorization handling
and optionally allow for a more fine-grained access control than required by the specification.

The VTN stores the data in a Postgres database,
but the code base is ready for using other data stores as well in the future.
Again, we warmly welcome contributions or sponsoring if you are interested in adding additional storage support.

The VEN is a library for conveniently interacting with the REST API provided by a VTN.
We aim for a clean and easy-to-understand API of the library to be used by business or VEN logic.
Additionally, we will use the library to create a CLI application for easy testing and prototyping, 
see [#52](https://github.com/OpenLEADR/openleadr-rs/issues/52) for the current progress.

## Contributing
We expect you to follow our [code of conduct](CODE_OF_CONDUCT.md) for any contribution.

If you are missing a feature or see unexpected behavior, 
do not hesitate to open an issue on our [GitHub](https://github.com/OpenLEADR/openleadr-rs) page.
If you suspect a security-critical issue, please refer to [`SECURITY.md`](SECURITY.md).

Additionally, we are happy to see pull requests on this repository as well.
We prefer to know when you intend to develop some functionality to make sure that there aren't multiple people working on the same issue. Simply drop a short note to the corresponding issue.

For your commits, please make sure you add a `signed-off-by` appendix to your commit message,
as the [LF energy contribution guidelines](https://tac.lfenergy.org/process/contribution_guidelines.html#developer-certificate-of-origin) require that.
By doing so, you acknowledge the text in [`CONTRIBUTING`](CONTRIBUTING).
The easiest way is to add a `-s` flag to the `git commit` command, i.e. use `git commit -s`.

If you are interested in contributing but don't know where to start, 
check out issues marked as [good first issue](https://github.com/OpenLEADR/openleadr-rs/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
or [help wanted](https://github.com/OpenLEADR/openleadr-rs/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22),
or simply open an issue and ask for good starting points.

## Sponsoring

If your organization relies on this project but cannot contribute directly to the code, please consider sponsoring ongoing development and maintenance or supporting the development of specific features your team requires.

For inquiries, please contact [Tweede golf](https://tweedegolf.nl/en/contact).


