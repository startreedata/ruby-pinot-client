#!/usr/bin/env ruby
require_relative "../lib/pinot"

client = Pinot.from_broker_list(["localhost:8000"])
resp = client.execute_sql("baseballStats", "SELECT playerName, runs FROM baseballStats LIMIT 5")
resp.result_table.row_count.times do |i|
  puts "#{resp.result_table.get_string(i, 0)}: #{resp.result_table.get_long(i, 1)} runs"
end
