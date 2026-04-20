module Pinot
  module PreparedStatement
    # Duck-type interface — implementers must define:
    #   set_string, set_int, set_int64, set_float64, set_bool, set
    #   execute, execute_with_params
    #   clear_parameters, close
    #   get_query, get_parameter_count
  end

  class PreparedStatementImpl
    include PreparedStatement

    def initialize(connection:, table:, query_template:)
      @connection = connection
      @table = table
      @query_template = query_template
      @parts = query_template.split("?", -1)
      @param_count = @parts.length - 1
      @parameters = Array.new(@param_count)
      @mutex = Mutex.new
      @closed = false
    end

    def get_query
      @query_template
    end

    def get_parameter_count
      @param_count
    end

    def set_string(index, value)
      set(index, value.to_s)
    end

    def set_int(index, value)
      set(index, value.to_i)
    end

    def set_int64(index, value)
      set(index, value.to_i)
    end

    def set_float64(index, value)
      set(index, value.to_f)
    end

    def set_bool(index, value)
      set(index, !!value)
    end

    def set(index, value)
      @mutex.synchronize do
        raise PreparedStatementClosedError, "prepared statement is closed" if @closed
        raise "parameter index #{index} is out of range [1, #{@param_count}]" unless index.between?(1, @param_count)

        @parameters[index - 1] = value
      end
      nil
    end

    def execute(headers: {})
      @mutex.synchronize do
        raise PreparedStatementClosedError, "prepared statement is closed" if @closed

        @parameters.each_with_index do |p, i|
          raise "parameter at index #{i + 1} is not set" if p.nil?
        end
      end
      query = begin
        build_query(@parameters)
      rescue StandardError => e
        raise "failed to build query: #{e.message}"
      end
      @connection.execute_sql(@table, query, headers: headers)
    end

    def execute_with_params(*params, headers: {})
      @mutex.synchronize { raise PreparedStatementClosedError, "prepared statement is closed" if @closed }
      raise "expected #{@param_count} parameters, got #{params.length}" if params.length != @param_count

      query = begin
        build_query(params)
      rescue StandardError => e
        raise "failed to build query: #{e.message}"
      end
      @connection.execute_sql(@table, query, headers: headers)
    end

    def clear_parameters
      @mutex.synchronize do
        raise PreparedStatementClosedError, "prepared statement is closed" if @closed

        @parameters.fill(nil)
      end
      nil
    end

    def close
      @mutex.synchronize do
        @closed = true
        @parameters = nil
      end
      nil
    end

    def build_query(params)
      raise "expected #{@param_count} parameters, got #{params.length}" if params.length != @param_count

      result = ""
      params.each_with_index do |param, i|
        formatted = begin
          @connection.format_arg(param)
        rescue StandardError => e
          raise "failed to format parameter: #{e.message}"
        end
        result += @parts[i] + formatted
      end
      result + @parts.last
    end
  end
end
