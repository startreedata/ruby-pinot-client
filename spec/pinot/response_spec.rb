RSpec.describe Pinot::BrokerResponse do
  let(:selection_json) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["INT","INT","INT","INT","INT","INT","INT","INT","INT","INT","STRING","INT","INT","STRING","STRING","INT","INT","INT","INT","INT","INT","INT","STRING","INT","INT"],"columnNames":["AtBatting","G_old","baseOnBalls","caughtStealing","doules","groundedIntoDoublePlays","hits","hitsByPitch","homeRuns","intentionalWalks","league","numberOfGames","numberOfGamesAsBatter","playerID","playerName","playerStint","runs","runsBattedIn","sacrificeFlies","sacrificeHits","stolenBases","strikeouts","teamID","tripples","yearID"]},"rows":[[0,11,0,0,0,0,0,0,0,0,"NL",11,11,"aardsda01","David Allan",1,0,0,0,0,0,0,"SFN",0,2004],[2,45,0,0,0,0,0,0,0,0,"NL",45,43,"aardsda01","David Allan",1,0,0,0,1,0,0,"CHN",0,2006],[0,2,0,0,0,0,0,0,0,0,"AL",25,2,"aardsda01","David Allan",1,0,0,0,0,0,0,"CHA",0,2007],[1,5,0,0,0,0,0,0,0,0,"AL",47,5,"aardsda01","David Allan",1,0,0,0,0,0,1,"BOS",0,2008],[0,0,0,0,0,0,0,0,0,0,"AL",73,3,"aardsda01","David Allan",1,0,0,0,0,0,0,"SEA",0,2009],[0,0,0,0,0,0,0,0,0,0,"AL",53,4,"aardsda01","David Allan",1,0,0,0,0,0,0,"SEA",0,2010],[0,0,0,0,0,0,0,0,0,0,"AL",1,0,"aardsda01","David Allan",1,0,0,0,0,0,0,"NYA",0,2012],[468,122,28,2,27,13,131,3,13,0,"NL",122,122,"aaronha01","Henry Louis",1,58,69,4,6,2,39,"ML1",6,1954],[602,153,49,1,37,20,189,3,27,5,"NL",153,153,"aaronha01","Henry Louis",1,105,106,4,7,3,61,"ML1",9,1955],[609,153,37,4,34,21,200,2,26,6,"NL",153,153,"aaronha01","Henry Louis",1,106,92,7,5,2,54,"ML1",14,1956]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numConsumingSegmentsQueried":0,"numDocsScanned":10,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":250,"numGroupsLimitReached":false,"totalDocs":97889,"timeUsedMs":6,"segmentStatistics":[],"traceInfo":{},"minConsumingFreshnessTimeMs":0}'
  end

  describe "SQL selection query response" do
    subject(:resp) { Pinot::BrokerResponse.from_json(selection_json) }

    it "parses stat fields" do
      expect(resp.aggregation_results.length).to eq 0
      expect(resp.exceptions.length).to eq 0
      expect(resp.min_consuming_freshness_time_ms).to eq 0
      expect(resp.num_consuming_segments_queried).to eq 0
      expect(resp.num_docs_scanned).to eq 10
      expect(resp.num_entries_scanned_in_filter).to eq 0
      expect(resp.num_entries_scanned_post_filter).to eq 250
      expect(resp.num_groups_limit_reached).to be false
      expect(resp.num_segments_matched).to eq 1
      expect(resp.num_segments_processed).to eq 1
      expect(resp.num_segments_queried).to eq 1
      expect(resp.num_servers_queried).to eq 1
      expect(resp.num_servers_responded).to eq 1
      expect(resp.result_table).not_to be_nil
      expect(resp.selection_results).to be_nil
      expect(resp.time_used_ms).to eq 6
      expect(resp.total_docs).to eq 97889
      expect(resp.trace_info.length).to eq 0
    end

    it "parses result table schema" do
      rt = resp.result_table
      expect(rt.row_count).to eq 10
      expect(rt.column_count).to eq 25

      expected_names = %w[AtBatting G_old baseOnBalls caughtStealing doules groundedIntoDoublePlays
                          hits hitsByPitch homeRuns intentionalWalks league numberOfGames
                          numberOfGamesAsBatter playerID playerName playerStint runs runsBattedIn
                          sacrificeFlies sacrificeHits stolenBases strikeouts teamID tripples yearID]
      expected_types = %w[INT INT INT INT INT INT INT INT INT INT STRING INT INT STRING STRING INT INT INT INT INT INT INT STRING INT INT]

      25.times do |i|
        expect(rt.column_name(i)).to eq expected_names[i]
        expect(rt.column_data_type(i)).to eq expected_types[i]
      end
    end
  end

  describe "SQL aggregation query response" do
    let(:json) do
      '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[97889]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numConsumingSegmentsQueried":0,"numDocsScanned":97889,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":0,"numGroupsLimitReached":false,"totalDocs":97889,"timeUsedMs":5,"segmentStatistics":[],"traceInfo":{},"minConsumingFreshnessTimeMs":0}'
    end

    subject(:resp) { Pinot::BrokerResponse.from_json(json) }

    it "parses result table" do
      rt = resp.result_table
      expect(rt.row_count).to eq 1
      expect(rt.column_count).to eq 1
      expect(rt.column_name(0)).to eq "cnt"
      expect(rt.column_data_type(0)).to eq "LONG"
      expect(rt.get(0, 0)).to eq Pinot::JsonNumber.new("97889")
      expect(rt.get_int(0, 0)).to eq 97889
      expect(rt.get_long(0, 0)).to eq 97889
      expect(rt.get_float(0, 0)).to eq 97889.0
      expect(rt.get_double(0, 0)).to eq 97889.0
    end
  end

  describe "SQL aggregation group-by response" do
    let(:json) do
      '{"resultTable":{"dataSchema":{"columnDataTypes":["STRING","LONG","DOUBLE"],"columnNames":["teamID","cnt","sum_homeRuns"]},"rows":[["ANA",337,1324.0],["BL2",197,136.0],["ARI",727,2715.0],["BL1",48,24.0],["ALT",17,2.0],["ATL",1951,7312.0],["BFN",122,105.0],["BL3",36,32.0],["BFP",26,20.0],["BAL",2380,9164.0]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numConsumingSegmentsQueried":0,"numDocsScanned":97889,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":195778,"numGroupsLimitReached":true,"totalDocs":97889,"timeUsedMs":24,"segmentStatistics":[],"traceInfo":{},"minConsumingFreshnessTimeMs":0}'
    end

    subject(:resp) { Pinot::BrokerResponse.from_json(json) }

    it "parses group-by result table" do
      rt = resp.result_table
      expect(rt.row_count).to eq 10
      expect(rt.column_count).to eq 3
      expect(rt.column_name(0)).to eq "teamID"
      expect(rt.column_data_type(0)).to eq "STRING"
      expect(rt.column_name(1)).to eq "cnt"
      expect(rt.column_data_type(1)).to eq "LONG"
      expect(rt.column_name(2)).to eq "sum_homeRuns"
      expect(rt.column_data_type(2)).to eq "DOUBLE"

      expect(rt.get_string(0, 0)).to eq "ANA"
      expect(rt.get_long(0, 1)).to eq 337
      expect(rt.get_double(0, 2)).to eq 1324.0

      expect(rt.get_string(1, 0)).to eq "BL2"
      expect(rt.get_long(1, 1)).to eq 197
      expect(rt.get_double(1, 2)).to eq 136.0
    end

    it "parses num_groups_limit_reached" do
      expect(resp.num_groups_limit_reached).to be true
    end
  end

  describe "wrong type response (overflow/infinity)" do
    let(:json) do
      '{"resultTable":{"dataSchema":{"columnDataTypes":["STRING","LONG","DOUBLE"],"columnNames":["teamID","cnt","sum_homeRuns"]},"rows":[["ANA",9223372036854775808, 1e309]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numConsumingSegmentsQueried":0,"numDocsScanned":97889,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":195778,"numGroupsLimitReached":true,"totalDocs":97889,"timeUsedMs":24,"segmentStatistics":[],"traceInfo":{},"minConsumingFreshnessTimeMs":0}'
    end

    subject(:resp) { Pinot::BrokerResponse.from_json(json) }

    it "returns 0 for overflow values" do
      rt = resp.result_table
      expect(rt.get_string(0, 0)).to eq "ANA"
      expect(rt.get_int(0, 1)).to eq 0
      expect(rt.get_long(0, 1)).to eq 0
      expect(rt.get_float(0, 2)).to eq 0.0
      expect(rt.get_double(0, 2)).to eq 0.0
    end
  end

  describe "exception response" do
    let(:json) do
      '{"resultTable":{"dataSchema":{"columnDataTypes":["DOUBLE"],"columnNames":["max(league)"]},"rows":[]},"exceptions":[{"errorCode":200,"message":"QueryExecutionError:\\njava.lang.NumberFormatException: For input string: \\"UA\\""}],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":0,"numSegmentsMatched":0,"numConsumingSegmentsQueried":0,"numDocsScanned":0,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":0,"numGroupsLimitReached":false,"totalDocs":97889,"timeUsedMs":5,"segmentStatistics":[],"traceInfo":{},"minConsumingFreshnessTimeMs":0}'
    end

    subject(:resp) { Pinot::BrokerResponse.from_json(json) }

    it "parses exceptions" do
      expect(resp.exceptions.length).to eq 1
      expect(resp.exceptions[0].error_code).to eq 200
      expect(resp.exceptions[0].message).to include("QueryExecutionError:")
    end

    it "parses empty result table" do
      rt = resp.result_table
      expect(rt.row_count).to eq 0
      expect(rt.column_count).to eq 1
      expect(rt.column_name(0)).to eq "max(league)"
      expect(rt.column_data_type(0)).to eq "DOUBLE"
    end
  end
end

RSpec.describe Pinot::ResultTable do
  let(:result_table) do
    Pinot::ResultTable.new(
      "dataSchema" => {
        "columnDataTypes" => %w[INT LONG FLOAT DOUBLE STRING INT LONG FLOAT DOUBLE STRING],
        "columnNames" => %w[int_val long_val float_val double_val string_val decimal_int decimal_long large_float large_double non_number]
      },
      "rows" => [
        [123, 456789, 123.45, 789.123, 999, 42.0, 12345.0, 999999999999.0, 1.7976931348623157e+308, "not_a_number"],
        [9223372036854775808, -9223372036854775809, 1e309, -1e309, "string_value", 42.5, 123.7, 3.4028235e+39, Float::INFINITY, 123]
      ]
    )
  end

  describe "get methods — row 0 normal cases" do
    it "get_int for integer" do
      expect(result_table.get_int(0, 0)).to eq 123
    end

    it "get_long for long" do
      expect(result_table.get_long(0, 1)).to eq 456789
    end

    it "get_float for float" do
      expect(result_table.get_float(0, 2)).to eq 123.45.to_f
    end

    it "get_double for double" do
      expect(result_table.get_double(0, 3)).to eq 789.123
    end

    it "get_string for number cell" do
      expect(result_table.get_string(0, 4)).to eq "999"
    end

    it "decimal 42.0 converts to int 42" do
      expect(result_table.get_int(0, 5)).to eq 42
    end

    it "decimal 12345.0 converts to long 12345" do
      expect(result_table.get_long(0, 6)).to eq 12345
    end

    it "get_float for large number" do
      expect(result_table.get_float(0, 7)).to eq 999999999999.0
    end

    it "get_double for large double" do
      expect(result_table.get_double(0, 8)).to eq 1.7976931348623157e+308
    end
  end

  describe "non-JsonNumber (plain string) cell" do
    it "get_string returns string" do
      expect(result_table.get_string(0, 9)).to eq "not_a_number"
    end

    it "get_int returns 0" do
      expect(result_table.get_int(0, 9)).to eq 0
    end

    it "get_long returns 0" do
      expect(result_table.get_long(0, 9)).to eq 0
    end

    it "get_float returns 0" do
      expect(result_table.get_float(0, 9)).to eq 0.0
    end

    it "get_double returns 0" do
      expect(result_table.get_double(0, 9)).to eq 0.0
    end
  end

  describe "row 1 — edge cases" do
    it "out-of-range int64 returns 0 for get_long" do
      expect(result_table.get_long(1, 0)).to eq 0
      expect(result_table.get_long(1, 1)).to eq 0
    end

    it "infinity float returns 0 for get_float" do
      expect(result_table.get_float(1, 2)).to eq 0.0
    end

    it "negative infinity returns 0 for get_double" do
      expect(result_table.get_double(1, 3)).to eq 0.0
    end

    it "string_value returns correctly" do
      expect(result_table.get_string(1, 4)).to eq "string_value"
    end

    it "non-whole 42.5 returns 0 for get_int" do
      expect(result_table.get_int(1, 5)).to eq 0
    end

    it "non-whole 123.7 returns 0 for get_long" do
      expect(result_table.get_long(1, 6)).to eq 0
    end

    it "out-of-range float32 returns 0" do
      expect(result_table.get_float(1, 7)).to eq 0.0
    end

    it "infinity returns 0 for get_double" do
      expect(result_table.get_double(1, 8)).to eq 0.0
    end

    it "plain integer 123 wrapped as JsonNumber, get_string returns '123'" do
      expect(result_table.get_string(1, 9)).to eq "123"
    end

    it "plain integer 123 wrapped as JsonNumber, numeric getters return 0 (no... actually it IS a JsonNumber)" do
      # 123 is Numeric so it gets wrapped in JsonNumber("123")
      expect(result_table.get_int(1, 9)).to eq 123
      expect(result_table.get_long(1, 9)).to eq 123
    end
  end

  describe "get methods with malformed input" do
    let(:rt) do
      Pinot::ResultTable.new(
        "dataSchema" => {
          "columnDataTypes" => %w[INT STRING DOUBLE],
          "columnNames" => %w[invalid_json string_val valid_double]
        },
        "rows" => [["invalid_number_format", "test_string", 123.456]]
      )
    end

    it "malformed string cell - get_int returns 0" do
      # "invalid_number_format" is a String, not Numeric, so no JsonNumber wrapping
      expect(rt.get_int(0, 0)).to eq 0
    end

    it "malformed string cell - get_long returns 0" do
      expect(rt.get_long(0, 0)).to eq 0
    end

    it "malformed string cell - get_float returns 0" do
      expect(rt.get_float(0, 0)).to eq 0.0
    end

    it "malformed string cell - get_double returns 0" do
      expect(rt.get_double(0, 0)).to eq 0.0
    end

    it "regular string returns get_string" do
      expect(rt.get_string(0, 1)).to eq "test_string"
    end

    it "valid double returns get_double" do
      expect(rt.get_double(0, 2)).to eq 123.456
    end
  end

  describe "edge cases for numeric types" do
    let(:rt) do
      Pinot::ResultTable.new(
        "dataSchema" => {
          "columnDataTypes" => %w[INT INT LONG FLOAT FLOAT],
          "columnNames" => %w[int_overflow_float int_overflow_int long_overflow float_inf float_ok]
        },
        "rows" => [[2147483648.0, 2147483648, 9.223372036854776e19, Float::INFINITY, 3.14]]
      )
    end

    it "int32 max+1 as float returns 0" do
      expect(rt.get_int(0, 0)).to eq 0
    end

    it "int32 max+1 as integer returns 0" do
      expect(rt.get_int(0, 1)).to eq 0
    end

    it "beyond int64 range returns 0 for get_long" do
      expect(rt.get_long(0, 2)).to eq 0
    end

    it "infinity returns 0 for get_float" do
      expect(rt.get_float(0, 3)).to eq 0.0
    end

    it "valid float 3.14 returns correctly" do
      expect(rt.get_float(0, 4)).to be_within(0.001).of(3.14)
    end
  end

  describe "utility methods" do
    let(:rt) do
      Pinot::ResultTable.new(
        "dataSchema" => {
          "columnDataTypes" => %w[INT STRING DOUBLE],
          "columnNames" => %w[col1 col2 col3]
        },
        "rows" => [
          [1, "value1", 1.1],
          [2, "value2", 2.2]
        ]
      )
    end

    it "row_count" do
      expect(rt.row_count).to eq 2
    end

    it "column_count" do
      expect(rt.column_count).to eq 3
    end

    it "column_name" do
      expect(rt.column_name(0)).to eq "col1"
      expect(rt.column_name(1)).to eq "col2"
      expect(rt.column_name(2)).to eq "col3"
    end

    it "column_data_type" do
      expect(rt.column_data_type(0)).to eq "INT"
      expect(rt.column_data_type(1)).to eq "STRING"
      expect(rt.column_data_type(2)).to eq "DOUBLE"
    end

    it "get returns JsonNumber for numeric cells" do
      expect(rt.get(0, 0)).to eq Pinot::JsonNumber.new("1")
      expect(rt.get(0, 1)).to eq "value1"
      expect(rt.get(0, 2)).to eq Pinot::JsonNumber.new("1.1")
    end
  end

  describe "boundary values" do
    let(:rt) do
      Pinot::ResultTable.new(
        "dataSchema" => {
          "columnDataTypes" => %w[INT LONG FLOAT DOUBLE],
          "columnNames" => %w[int_max long_max float_max double_max]
        },
        "rows" => [
          [2147483647, 9223372036854775807, 3.4028234e+38, 1.7976931348623157e+308],
          [-2147483648, -9223372036854775808, -3.4028234e+38, -1.7976931348623157e+308]
        ]
      )
    end

    it "int32 max" do
      expect(rt.get_int(0, 0)).to eq 2147483647
    end

    it "int64 max" do
      expect(rt.get_long(0, 1)).to eq 9223372036854775807
    end

    it "float32 near max is non-zero" do
      result = rt.get_float(0, 2)
      expect(result).not_to eq 0.0
    end

    it "float64 max" do
      expect(rt.get_double(0, 3)).to eq 1.7976931348623157e+308
    end

    it "int32 min" do
      expect(rt.get_int(1, 0)).to eq(-2147483648)
    end

    it "int64 min" do
      expect(rt.get_long(1, 1)).to eq(-9223372036854775808)
    end

    it "float32 near min is non-zero" do
      result = rt.get_float(1, 2)
      expect(result).not_to eq 0.0
    end

    it "float64 min" do
      expect(rt.get_double(1, 3)).to eq(-1.7976931348623157e+308)
    end
  end
end
