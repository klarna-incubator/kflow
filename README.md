# KFlow
> Bullet-proof data stream processing framework

[![Build Status][ci-image]][ci-url]
[![License][license-image]][license-url]
[![Developed at Klarna][klarna-image]][klarna-url]

KFlow is an Erlang dataflow DSL built with Kafka in mind. Services
implemented using Kflow have the following properties:

 1. Stateless. Service can be restarted at any moment without risk of
    losing data
 1. Fault-tolerant and self-healing. Any crashes in the service can be
    fixed, and the service will automatically replay the data that
    caused crash, once the fix is deployed. This enables fearless A/B
    testing and canary deployments
 1. Scalable.
 1. Automated parallelization. Each transformation runs in parallel
 1. Automated backpressure.

This is achieved by rather sophisticated offset tracking logic built
in KFlow behaviors, that makes sure that consumer offsets are
committed to Kafka only when an input message is fully processed.

Another feature of KFlow is "virtual partition" that allows to split
Kafka partitions into however many substream that are processed
independently.

## Usage example

KFlow is configured using a special Erlang module named
`kflow_config.erl`. Every exported function in this module defines a
workflow. For example:

```
-module(kflow_config).

-export([example_workflow/0]).

example_workflow() ->
  %% Define a "pipe" (much like Unix pipe):
  PipeSpec = [ %% Parse messages:
               {map, fun(_Offset, #{key => KafkaKey, value => JSON} ->
                         (jsone:decode(JSON)) #{key => KafkaKey}
                     end}
               %% Create a "virtual partition" for each unique key:
             , {demux, fun(_Offset, #{key := Key}) ->
                           Key
                       end}
               %% Collect messages into chunks of 100 for faster processing:
             , {aggregate, kflow_buffer, #{max_messages => 100}}
               %% Dump chunks into Postgres:
             , {map, kflow_postgres, #{ database => #{ host => "localhost"
                                                     , ...
                                                     }
                                      , table  => "my_table"
                                      , fields => [key, foo, bar, baz]
                                      , keys   => [key]
                                      }}
             ],
  kflow:mk_kafka_workflow(?FUNCTION_NAME, PipeSpec,
                          #{ kafka_topic => <<"example_topic">>
                           , group_id    => <<"example_consumer_group_id">>
                           }).
```

_For more examples and usage, please refer to the [Docs](TODO)._

## Development setup

KFlow requires OTP21 or later and `rebar3` present in the
`PATH`. Build by running

```sh
make
```

## How to contribute

See our guide on [contributing](.github/CONTRIBUTING.md).

## Release History

See our [changelog](CHANGELOG.md).

## License

Copyright © 2020 Klarna Bank AB

For license details, see the [LICENSE](LICENSE) file in the root of this project.


<!-- Markdown link & img dfn's -->
[ci-image]: https://img.shields.io/badge/build-passing-brightgreen?style=flat-square
[ci-url]: https://github.com/klarna-incubator/TODO
[license-image]: https://img.shields.io/badge/license-Apache%202-blue?style=flat-square
[license-url]: http://www.apache.org/licenses/LICENSE-2.0
[klarna-image]: https://img.shields.io/badge/%20-Developed%20at%20Klarna-black?labelColor=ffb3c7&style=flat-square&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAOCAYAAAAmL5yKAAAAAXNSR0IArs4c6QAAAIRlWElmTU0AKgAAAAgABQESAAMAAAABAAEAAAEaAAUAAAABAAAASgEbAAUAAAABAAAAUgEoAAMAAAABAAIAAIdpAAQAAAABAAAAWgAAAAAAAALQAAAAAQAAAtAAAAABAAOgAQADAAAAAQABAACgAgAEAAAAAQAAABCgAwAEAAAAAQAAAA4AAAAA0LMKiwAAAAlwSFlzAABuugAAbroB1t6xFwAAAVlpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IlhNUCBDb3JlIDUuNC4wIj4KICAgPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4KICAgICAgPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIKICAgICAgICAgICAgeG1sbnM6dGlmZj0iaHR0cDovL25zLmFkb2JlLmNvbS90aWZmLzEuMC8iPgogICAgICAgICA8dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPgogICAgICA8L3JkZjpEZXNjcmlwdGlvbj4KICAgPC9yZGY6UkRGPgo8L3g6eG1wbWV0YT4KTMInWQAAAVBJREFUKBVtkz0vREEUhsdXgo5qJXohkUgQ0fgFNFpR2V5ClP6CQu9PiB6lEL1I7B9A4/treZ47c252s97k2ffMmZkz5869m1JKL/AFbzAHaiRbmsIf4BdaMAZqMFsOXNxXkroKbxCPV5l8yHOJLVipn9/vEreLa7FguSN3S2ynA/ATeQuI8tTY6OOY34DQaQnq9mPCDtxoBwuRxPfAvPMWnARlB12KAi6eLTPruOOP4gcl33O6+Sjgc83DJkRH+h2MgorLzaPy68W48BG2S+xYnmAa1L+nOxEduMH3fgjGFvZeVkANZau68B6CrgJxWosFFpF7iG+h5wKZqwt42qIJtARu/ix+gqsosEq8D35o6R3c7OL4lAnTDljEe9B3Qa2BYzmHemDCt6Diwo6JY7E+A82OnN9HuoBruAQvUQ1nSxP4GVzBDRyBfygf6RW2/gD3NmEv+K/DZgAAAABJRU5ErkJggg==
[klarna-url]: https://github.com/klarna-incubator
