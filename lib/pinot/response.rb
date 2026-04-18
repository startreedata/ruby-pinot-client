require "bigdecimal"

module Pinot
  INT32_MAX =  2_147_483_647
  INT32_MIN = -2_147_483_648
  INT64_MAX =  9_223_372_036_854_775_807
  INT64_MIN = -9_223_372_036_854_775_808
  FLOAT32_MAX = 3.4028235e+38

  # Mirrors Go's json.Number — stores the raw string representation of a JSON number.
  class JsonNumber
    attr_reader :raw

    def initialize(raw)
      @raw = raw.to_s
    end

    def to_s
      @raw
    end

    def ==(other)
      case other
      when JsonNumber then @raw == other.raw
      else false
      end
    end
  end

  class RespSchema
    attr_reader :column_names, :column_data_types

    def initialize(hash)
      @column_names = hash["columnNames"] || []
      @column_data_types = hash["columnDataTypes"] || []
    end
  end

  class PinotException
    attr_reader :error_code, :message

    def initialize(hash)
      @error_code = hash["errorCode"]
      @message = hash["message"]
    end
  end

  class SelectionResults
    attr_reader :columns, :results

    def initialize(hash)
      @columns = hash["columns"] || []
      @results = hash["results"] || []
    end
  end

  class AggregationResult
    attr_reader :function, :value, :group_by_columns, :group_by_result

    def initialize(hash)
      @function = hash["function"]
      @value = hash["value"]
      @group_by_columns = hash["groupByColumns"]
      @group_by_result = hash["groupByResult"]
    end
  end

  class ResultTable
    attr_reader :data_schema, :rows

    def initialize(hash)
      @data_schema = RespSchema.new(hash["dataSchema"] || {})
      raw_rows = hash["rows"] || []
      @rows = raw_rows.map do |row|
        row.map { |cell| cell.is_a?(Numeric) ? JsonNumber.new(cell) : cell }
      end
    end

    def row_count
      @rows.length
    end

    def column_count
      @data_schema.column_names.length
    end

    def column_name(i)
      @data_schema.column_names[i]
    end

    def column_data_type(i)
      @data_schema.column_data_types[i]
    end

    def get(row, col)
      @rows[row][col]
    end

    def get_string(row, col)
      @rows[row][col].to_s
    end

    def get_int(row, col)
      cell = @rows[row][col]
      return 0 unless cell.is_a?(JsonNumber)

      raw = cell.raw
      begin
        # Try parsing as integer first
        if raw.include?(".") || raw.include?("e") || raw.include?("E")
          # Floating point string — check if it's a whole number
          bd = BigDecimal(raw)
          return 0 if bd.infinite? || bd.nan? rescue return 0
          int_val = bd.to_i
          return 0 unless bd == BigDecimal(int_val.to_s)
          return 0 if int_val > INT32_MAX || int_val < INT32_MIN
          int_val.to_i
        else
          int_val = Integer(raw)
          return 0 if int_val > INT32_MAX || int_val < INT32_MIN
          int_val
        end
      rescue ArgumentError, TypeError
        0
      end
    end

    def get_long(row, col)
      cell = @rows[row][col]
      return 0 unless cell.is_a?(JsonNumber)

      raw = cell.raw
      begin
        if raw.include?(".") || raw.include?("e") || raw.include?("E")
          bd = BigDecimal(raw)
          return 0 if bd.infinite? || bd.nan? rescue return 0
          int_val = bd.to_i
          return 0 unless bd == BigDecimal(int_val.to_s)
          return 0 if int_val > INT64_MAX || int_val < INT64_MIN
          int_val
        else
          int_val = Integer(raw)
          return 0 if int_val > INT64_MAX || int_val < INT64_MIN
          int_val
        end
      rescue ArgumentError, TypeError
        0
      end
    end

    def get_float(row, col)
      cell = @rows[row][col]
      return 0.0 unless cell.is_a?(JsonNumber)

      raw = cell.raw
      begin
        f = Float(raw)
        return 0.0 if f.infinite? || f.nan?
        f32 = f.to_f
        return 0.0 if f32.abs > FLOAT32_MAX
        f32
      rescue ArgumentError, TypeError
        0.0
      end
    end

    def get_double(row, col)
      cell = @rows[row][col]
      return 0.0 unless cell.is_a?(JsonNumber)

      raw = cell.raw
      begin
        f = Float(raw)
        return 0.0 if f.infinite? || f.nan?
        f
      rescue ArgumentError, TypeError
        0.0
      end
    end
  end

  class BrokerResponse
    attr_reader :selection_results, :result_table, :aggregation_results,
                :exceptions, :trace_info,
                :num_segments_processed, :num_servers_responded,
                :num_segments_queried, :num_servers_queried,
                :num_segments_matched, :num_consuming_segments_queried,
                :num_docs_scanned, :num_entries_scanned_in_filter,
                :num_entries_scanned_post_filter, :total_docs,
                :time_used_ms, :min_consuming_freshness_time_ms,
                :num_groups_limit_reached

    def self.from_json(json_str)
      hash = JSON.parse(json_str)
      new(hash)
    end

    def initialize(hash)
      @selection_results = hash["selectionResults"] ? SelectionResults.new(hash["selectionResults"]) : nil
      @result_table = hash["resultTable"] ? ResultTable.new(hash["resultTable"]) : nil
      @aggregation_results = (hash["aggregationResults"] || []).map { |r| AggregationResult.new(r) }
      @exceptions = (hash["exceptions"] || []).map { |e| PinotException.new(e) }
      @trace_info = hash["traceInfo"] || {}

      @num_servers_queried = hash["numServersQueried"] || 0
      @num_servers_responded = hash["numServersResponded"] || 0
      @num_segments_queried = hash["numSegmentsQueried"] || 0
      @num_segments_processed = hash["numSegmentsProcessed"] || 0
      @num_segments_matched = hash["numSegmentsMatched"] || 0
      @num_consuming_segments_queried = hash["numConsumingSegmentsQueried"] || 0
      @num_docs_scanned = hash["numDocsScanned"] || 0
      @num_entries_scanned_in_filter = hash["numEntriesScannedInFilter"] || 0
      @num_entries_scanned_post_filter = hash["numEntriesScannedPostFilter"] || 0
      @num_groups_limit_reached = hash["numGroupsLimitReached"] || false
      @total_docs = hash["totalDocs"] || 0
      @time_used_ms = hash["timeUsedMs"] || 0
      @min_consuming_freshness_time_ms = hash["minConsumingFreshnessTimeMs"] || 0
    end
  end
end
