module Pinot
  class Paginator
    include Enumerable

    DEFAULT_PAGE_SIZE = 1000

    def initialize(connection, table, query, page_size:, query_timeout_ms: nil)
      if query.match?(/\b(LIMIT|OFFSET)\b/i)
        raise ArgumentError, "base query must not contain LIMIT or OFFSET; the paginator controls them"
      end
      raise ArgumentError, "page_size must be positive" unless page_size.is_a?(Integer) && page_size > 0

      @connection       = connection
      @table            = table
      @base_query       = query.strip
      @page_size        = page_size
      @query_timeout_ms = query_timeout_ms
    end

    # Yields each page's BrokerResponse. Returns an Enumerator if no block given.
    def each_page
      return enum_for(:each_page) unless block_given?

      offset = 0
      loop do
        paged_query = "#{@base_query} LIMIT #{@page_size} OFFSET #{offset}"
        response    = @connection.execute_sql(@table, paged_query, query_timeout_ms: @query_timeout_ms)
        rows        = response.result_table&.rows || []

        yield response if rows.any?

        break if rows.size < @page_size

        offset += @page_size
      end
    end

    # Yields each row (Array) across all pages. Returns an Enumerator if no block given.
    # Including Enumerable delegates .map/.select/.to_a etc. here.
    def each(&block)
      return enum_for(:each) unless block_given?

      each_page do |response|
        response.result_table.rows.each(&block)
      end
    end

    alias each_row each
  end
end
