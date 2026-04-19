#!/usr/bin/env ruby
require_relative "../lib/pinot"

client = Pinot.from_controller("localhost:9000")
resp = client.execute_sql("baseballStats", "SELECT count(*) FROM baseballStats")
puts "Total rows: #{resp.result_table.get_long(0, 0)}"
