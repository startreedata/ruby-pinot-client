#!/usr/bin/env ruby
require_relative "../lib/pinot"

Pinot::Instrumentation.on_query = lambda do |event|
  status = event[:success] ? "OK" : "ERR"
  puts "[#{status}] #{event[:table]} — #{event[:duration_ms].round(2)}ms"
end

client = Pinot.from_broker_list(["localhost:8000"])
client.execute_sql("baseballStats", "SELECT count(*) FROM baseballStats")
