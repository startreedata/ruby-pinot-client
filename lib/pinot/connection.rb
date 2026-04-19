require "bigdecimal"

module Pinot
  class Connection
    attr_accessor :query_timeout_ms

    def initialize(transport:, broker_selector:, use_multistage_engine: false, logger: nil,
                   query_timeout_ms: nil, circuit_breaker_registry: nil)
      @transport = transport
      @broker_selector = broker_selector
      @use_multistage_engine = use_multistage_engine
      @trace = false
      @logger = logger
      @query_timeout_ms = query_timeout_ms
      @circuit_breaker_registry = circuit_breaker_registry
    end

    def use_multistage_engine=(val)
      @use_multistage_engine = val
    end

    def open_trace
      @trace = true
    end

    def close_trace
      @trace = false
    end

    def execute_sql(table, query, query_timeout_ms: nil, headers: {})
      Pinot::Instrumentation.instrument(table: table, query: query) do
        logger.debug "Executing SQL on table=#{table}: #{query}"
        broker = @broker_selector.select_broker(table)
        effective_timeout = query_timeout_ms || @query_timeout_ms
        run_with_circuit_breaker(broker) do
          @transport.execute(broker, build_request(query, timeout_ms: effective_timeout),
                             extra_request_headers: headers)
        end
      end
    end

    def execute_sql_with_timeout(table, query, timeout_ms)
      execute_sql(table, query, query_timeout_ms: timeout_ms)
    end

    def execute_sql_with_params(table, query_pattern, params, query_timeout_ms: nil, headers: {})
      query = format_query(query_pattern, params)
      execute_sql(table, query, query_timeout_ms: query_timeout_ms, headers: headers)
    rescue => e
      raise e
    end

    def execute_many(queries, max_concurrency: nil)
      return [] if queries.empty?

      results  = Array.new(queries.size)
      # Queue acts as a counting semaphore: pre-filled with N tokens.
      sem = max_concurrency ? build_semaphore(max_concurrency) : nil

      threads = queries.each_with_index.map do |item, idx|
        table      = item[:table]           || item["table"]           || ""
        query      = item[:query]           || item["query"]           || ""
        timeout_ms = item[:query_timeout_ms] || item["query_timeout_ms"]

        Thread.new do
          sem&.pop   # acquire
          begin
            resp = execute_sql(table, query, query_timeout_ms: timeout_ms)
            results[idx] = QueryResult.new(table: table, query: query, response: resp, error: nil)
          rescue => e
            results[idx] = QueryResult.new(table: table, query: query, response: nil, error: e)
          ensure
            sem&.push(:token)  # release
          end
        end
      end

      threads.each(&:join)
      results
    end

    def paginate(table, query, page_size: Paginator::DEFAULT_PAGE_SIZE, query_timeout_ms: nil)
      Paginator.new(self, table, query, page_size: page_size, query_timeout_ms: query_timeout_ms)
    end

    def prepare(table, query_template)
      raise ArgumentError, "table name cannot be empty" if table.nil? || table.strip.empty?
      raise ArgumentError, "query template cannot be empty" if query_template.nil? || query_template.strip.empty?
      count = query_template.count("?")
      raise ArgumentError, "query template must contain at least one parameter placeholder (?)" if count == 0
      PreparedStatementImpl.new(connection: self, table: table, query_template: query_template)
    end

    def format_query(pattern, params)
      params ||= []
      placeholders = pattern.count("?")
      if placeholders != params.length
        raise "failed to format query: number of placeholders in queryPattern (#{placeholders}) does not match number of params (#{params.length})"
      end
      parts = pattern.split("?", -1)
      result = ""
      params.each_with_index do |param, i|
        formatted = begin
          format_arg(param)
        rescue => e
          raise "failed to format query: failed to format parameter: #{e.message}"
        end
        result += parts[i] + formatted
      end
      result + parts.last
    end

    def format_arg(value)
      case value
      when String
        "'#{value.gsub("'", "''")}'"
      when Integer
        value.to_s
      when Float
        value.to_s
      when TrueClass, FalseClass
        value.to_s
      when BigDecimal
        s = value.to_s("F")
        # Strip trailing .0 for whole numbers (mirrors Go's big.Int quoted format)
        s = s.sub(/\.0\z/, "") if s.end_with?(".0")
        "'#{s}'"
      when Time
        "'#{value.utc.strftime("%Y-%m-%d %H:%M:%S.") + format("%03d", value.utc.subsec * 1000)}'"
      else
        raise "unsupported type: #{value.class}"
      end
    end

    private

    def build_semaphore(n)
      q = SizedQueue.new(n)
      n.times { q.push(:token) }
      q
    end

    def run_with_circuit_breaker(broker, &block)
      return yield unless @circuit_breaker_registry
      @circuit_breaker_registry.for(broker).call(broker, &block)
    end

    def logger
      @logger || Pinot::Logging.logger
    end

    def build_request(query, timeout_ms: @query_timeout_ms)
      Request.new("sql", query, @trace, @use_multistage_engine, timeout_ms)
    end
  end
end
