require "bigdecimal"

module Pinot
  # Main entry point for querying Apache Pinot over HTTP.
  #
  # Build a Connection via the factory helpers rather than instantiating directly:
  #
  #   # Static broker list
  #   conn = Pinot.from_broker_list(["broker1:8099", "broker2:8099"])
  #
  #   # Controller-managed broker discovery
  #   conn = Pinot.from_controller("controller:9000")
  #
  #   # Full configuration
  #   conn = Pinot.from_config(Pinot::ClientConfig.new(
  #     broker_list:             ["broker:8099"],
  #     query_timeout_ms:        5_000,
  #     use_multistage_engine:   true,
  #     max_retries:             2,
  #     circuit_breaker_enabled: true
  #   ))
  class Connection
    # @return [Integer, nil] default query timeout in milliseconds; can be overridden per-call
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

    # Execute a SQL query against +table+ and return a BrokerResponse.
    #
    # @param table           [String]  Pinot table name (used for broker selection)
    # @param query           [String]  SQL query string
    # @param query_timeout_ms [Integer, nil] per-call timeout override (ms); overrides
    #                        the connection-level query_timeout_ms
    # @param headers         [Hash]    extra HTTP headers merged into this request only
    # @return [BrokerResponse]
    # @raise [BrokerNotFoundError]     no broker available for the table
    # @raise [QueryTimeoutError]       query exceeded the timeout
    # @raise [BrokerUnavailableError]  broker returned 503/504
    # @raise [TransportError]          other non-200 HTTP response
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

    # Convenience wrapper around execute_sql with an explicit timeout.
    # Equivalent to execute_sql(table, query, query_timeout_ms: timeout_ms).
    def execute_sql_with_timeout(table, query, timeout_ms)
      execute_sql(table, query, query_timeout_ms: timeout_ms)
    end

    # Execute a parameterised query by substituting +params+ into +query_pattern+.
    # Each +?+ placeholder in the pattern is replaced by the corresponding value
    # using safe type-aware formatting (strings are quoted and escaped).
    #
    # @param table         [String]  Pinot table name
    # @param query_pattern [String]  SQL template, e.g. "SELECT * FROM t WHERE id = ?"
    # @param params        [Array]   ordered values; supported types: String, Integer,
    #                                Float, TrueClass, FalseClass, BigDecimal, Time
    # @param query_timeout_ms [Integer, nil] per-call timeout override (ms)
    # @param headers       [Hash]    extra HTTP headers for this request
    # @return [BrokerResponse]
    # @raise [RuntimeError] placeholder / param count mismatch or unsupported type
    def execute_sql_with_params(table, query_pattern, params, query_timeout_ms: nil, headers: {})
      query = format_query(query_pattern, params)
      execute_sql(table, query, query_timeout_ms: query_timeout_ms, headers: headers)
    rescue => e
      raise e
    end

    # Execute multiple queries in parallel and return results in the same order.
    #
    # Each element of +queries+ must be a Hash with keys +:table+ and +:query+
    # (Strings or Symbols). An optional +:query_timeout_ms+ key overrides the
    # per-query timeout.
    #
    # Each slot in the returned Array is a QueryResult with either a +response+ or
    # an +error+ — failures are isolated so one bad query does not raise for the
    # whole batch.
    #
    #   results = conn.execute_many([
    #     { table: "orders",   query: "SELECT count(*) FROM orders" },
    #     { table: "products", query: "SELECT count(*) FROM products" }
    #   ], max_concurrency: 4)
    #
    #   results.each do |r|
    #     puts r.success? ? r.response.result_table.get_long(0, 0) : r.error.message
    #   end
    #
    # @param queries         [Array<Hash>]  query descriptors
    # @param max_concurrency [Integer, nil] maximum simultaneous in-flight queries;
    #                        nil means unlimited
    # @return [Array<QueryResult>]
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

    # Return a Paginator for cursor-based iteration over large result sets.
    #
    # The query must include a LIMIT clause; the broker stores the result set
    # and returns it in +page_size+ row slices on demand.
    #
    #   paginator = conn.paginate("SELECT * FROM myTable LIMIT 50000", page_size: 500)
    #   paginator.each_row { |row| puts row.map(&:to_s).join(", ") }
    #
    # @param query         [String]  SQL query (should include LIMIT)
    # @param page_size     [Integer] rows per page (default Paginator::DEFAULT_PAGE_SIZE = 1000)
    # @param table         [String, nil] used only for broker selection; nil picks any broker
    # @param extra_headers [Hash]    merged into every HTTP request of this cursor session
    # @return [Paginator]
    def paginate(query, page_size: Paginator::DEFAULT_PAGE_SIZE, table: nil, extra_headers: {})
      broker = @broker_selector.select_broker(table || "")
      Paginator.new(
        @transport.http_client,
        broker,
        query,
        page_size:     page_size,
        extra_headers: extra_headers
      )
    end

    # Create a PreparedStatement from a query template with +?+ placeholders.
    #
    #   stmt = conn.prepare("myTable", "SELECT * FROM myTable WHERE id = ? AND name = ?")
    #   stmt.set(1, 42)
    #   stmt.set(2, "Alice")
    #   resp = stmt.execute
    #
    # @param table          [String] Pinot table name (non-empty)
    # @param query_template [String] SQL with one or more +?+ placeholders
    # @return [PreparedStatementImpl]
    # @raise [ArgumentError] if table or query_template is blank, or contains no placeholders
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
