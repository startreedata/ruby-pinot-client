#!/usr/bin/env ruby
require_relative "../lib/pinot"

config = Pinot::ClientConfig.new(
  broker_list: ["localhost:8000"],
  use_multistage_engine: true
)
client = Pinot.from_config(config)
resp = client.execute_sql("baseballStats",
  "SELECT playerName, SUM(runs) as total_runs FROM baseballStats GROUP BY playerName ORDER BY total_runs DESC LIMIT 5")
resp.result_table.row_count.times do |i|
  puts "#{resp.result_table.get_string(i, 0)}: #{resp.result_table.get_long(i, 1)}"
end
