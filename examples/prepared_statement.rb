#!/usr/bin/env ruby
require_relative "../lib/pinot"

client = Pinot.from_broker_list(["localhost:8000"])
stmt = client.prepare("baseballStats", "SELECT playerName FROM baseballStats WHERE yearID = ? AND runs > ? LIMIT 10")
stmt.set(1, 2000)
stmt.set(2, 50)
resp = stmt.execute
resp.result_table.row_count.times { |i| puts resp.result_table.get_string(i, 0) }
stmt.close
