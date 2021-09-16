# feat

Library for extracting features (values) from data.

Primary usage: to achieve idempotency of requests to a service and detect conflicts.


## Build

To compile the project:

    make

To run all the checks (used in Github Actions CI) locally, [act](https://github.com/nektos/act) and Docker is required:

    act


## TODO

- [ ] Big TODO: runtime schema validator (can be used during testing)
  - schema format
  - conflicting nested schemas in map schemas under the same name (see example below)

Example of conflict in nested schemas:
```erlang
#{1 => {<<"a">>, #{11 => <<"b">>}},
  2 => {<<"a">>, {set, #{...}}}}
```
This is incorrect because a set and a map can't be stored under the same key.
Although generally this behaviour is desirable (for extra flexibility in complex structures),
cases like above must be checked.
