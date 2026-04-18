# Ruby Pinot Client

[![Build Status](https://github.com/startreedata/ruby-pinot-client/actions/workflows/tests.yml/badge.svg)](https://github.com/startreedata/ruby-pinot-client/actions/workflows/tests.yml)
[![Gem Version](https://img.shields.io/github/v/release/startreedata/ruby-pinot-client?label=gem)](https://github.com/startreedata/ruby-pinot-client/releases)

A Ruby client library for [Apache Pinot](https://pinot.apache.org/). Mirrors the API of the [Go client](https://github.com/startreedata/pinot-client-go) and supports HTTP JSON transport, multiple broker discovery strategies, and parameterized queries.

## Installation

Add to your `Gemfile`:

```ruby
gem "pinot", "~> 1.0"
```

Or install directly:

```bash
gem install pinot
```

## Quick Start

Start a local Pinot cluster using Docker:

```bash
docker run -d \
  --name pinot-quickstart \
  -p 8000:8000 \
  apachepinot/pinot:1.5.0 QuickStart -type BATCH
```

Then query it from Ruby:

```ruby
require "pinot"

client = Pinot.from_broker_list(["localhost:8000"])
resp   = client.execute_sql("baseballStats", "SELECT count(*) AS cnt FROM baseballStats")
puts resp.result_table.get_long(0, 0)  # => 97889
```

## Creating a Connection

### From a broker list

```ruby
client = Pinot.from_broker_list(["localhost:8000"])
```

For HTTPS brokers, include the scheme:

```ruby
client = Pinot.from_broker_list(["https://pinot-broker.example.com"])
```

### From a controller (dynamic broker discovery)

The client periodically polls the controller's `/v2/brokers/tables` API and automatically picks up broker changes.

```ruby
client = Pinot.from_controller("localhost:9000")
```

### From a `ClientConfig`

```ruby
config = Pinot::ClientConfig.new(
  broker_list:           ["localhost:8000"],
  http_timeout:          10,          # seconds
  extra_http_header:     { "Authorization" => "Bearer <token>" },
  use_multistage_engine: false,
  controller_config: Pinot::ControllerConfig.new(
    controller_address:            "localhost:9000",
    update_freq_ms:                1000,
    extra_controller_api_headers:  { "Authorization" => "Bearer <token>" }
  )
)
client = Pinot.from_config(config)
```

## Executing Queries

### Simple SQL

```ruby
resp = client.execute_sql("baseballStats", "SELECT playerName, homeRuns FROM baseballStats LIMIT 5")
```

### Parameterized queries

Use `?` placeholders — the client substitutes and quotes values safely:

```ruby
resp = client.execute_sql_with_params(
  "baseballStats",
  "SELECT playerName FROM baseballStats WHERE teamID = ? AND yearID >= ?",
  ["SFN", 2000]
)
```

### Multistage engine

```ruby
client.use_multistage_engine = true
resp = client.execute_sql("baseballStats", "SELECT teamID, count(*) FROM baseballStats GROUP BY teamID")
```

### Trace

```ruby
client.open_trace
resp = client.execute_sql("baseballStats", "SELECT count(*) FROM baseballStats")
client.close_trace
```

## Reading Results

`execute_sql` returns a `Pinot::BrokerResponse`. Results are in `result_table`:

```ruby
rt = resp.result_table

rt.row_count          # => number of rows
rt.column_count       # => number of columns
rt.column_name(0)     # => "playerName"
rt.column_data_type(0)# => "STRING"

rt.get(0, 0)          # => raw cell value (Pinot::JsonNumber or String)
rt.get_string(0, 0)   # => String
rt.get_int(0, 1)      # => Integer (32-bit)
rt.get_long(0, 1)     # => Integer (64-bit)
rt.get_float(0, 2)    # => Float (32-bit range)
rt.get_double(0, 2)   # => Float (64-bit)
```

Stat fields on `BrokerResponse`:

```ruby
resp.time_used_ms
resp.num_docs_scanned
resp.total_docs
resp.exceptions         # => Array<Pinot::PinotException>
resp.num_groups_limit_reached
```

## Prepared Statements

Prepared statements are thread-safe and can be reused with different parameters.

```ruby
stmt = client.prepare(
  "baseballStats",
  "SELECT playerName, sum(homeRuns) AS hr FROM baseballStats WHERE teamID = ? AND yearID >= ? GROUP BY playerName ORDER BY hr DESC LIMIT ?"
)

stmt.set_string(1, "NYA")
stmt.set_int(2, 2000)
stmt.set_int(3, 10)
resp = stmt.execute

# Or pass params inline:
resp = stmt.execute_with_params("BOS", 2000, 5)

# Reuse with new parameters:
stmt.clear_parameters
stmt.set_string(1, "LAA")
stmt.set_int(2, 2005)
stmt.set_int(3, 3)
resp = stmt.execute

stmt.close
```

### Supported parameter types

| Ruby type    | SQL rendering          |
|--------------|------------------------|
| `String`     | `'value'` (single-quote escaped) |
| `Integer`    | `42`                   |
| `Float`      | `3.14`                 |
| `TrueClass` / `FalseClass` | `true` / `false` |
| `BigDecimal` | `'1234567890'`         |
| `Time`       | `'2023-01-01 12:00:00.000'` |

## Running Tests

### Unit tests

```bash
bundle install
bundle exec rspec spec/pinot/
```

### Integration tests (requires a running Pinot cluster)

```bash
docker run -d --name pinot-quickstart -p 8000:8000 \
  apachepinot/pinot:1.5.0 QuickStart -type BATCH

# wait ~2 minutes for the cluster to load data, then:
bundle exec rspec spec/integration/ --format documentation
```

Environment variables:

| Variable      | Default     | Description             |
|---------------|-------------|-------------------------|
| `BROKER_HOST` | `127.0.0.1` | Pinot broker hostname   |
| `BROKER_PORT` | `8000`      | Pinot broker HTTP port  |

## License

Apache License 2.0 — see [LICENSE](LICENSE).
