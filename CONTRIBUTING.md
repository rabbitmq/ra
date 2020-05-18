# Contributing to Ra

## Overview

Ra is still a maturing project under development, so consider discussing your idea with
the maintainers on the RabbitMQ mailing list, rabbitmq-users.

The process is fairly standard and straightforward:

 * Fork the repository
 * Create a branch for your changes
 * Add tests, modify code, refactor, repeat
 * Push your branch
 * Submit a pull request with a reasonably detailed justification of your changes
 * Be patient


## Building

Ra uses [erlang.mk](https://erlang.mk/) for build system. Build it with

``` shell
make
```

Clean compilation artifacts with

``` shell
make clean
```

## Running Tests

```
make tests
```

and then open `logs/index.html` to see test run results.
