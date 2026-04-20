module Pinot
  # Cursor-based pagination using Pinot's native server-side cursor API.
  #
  # The broker stores the full result set and returns slices on demand.
  # All fetch requests after the first are pinned to the broker that owns
  # the cursor (brokerHost:brokerPort from the initial response), ensuring
  # correct broker affinity.
  #
  # == Obtaining a Paginator
  #
  #   paginator = conn.paginate(
  #     "SELECT * FROM myTable WHERE col > 0",
  #     page_size:     500,    # rows per page (default 1000)
  #     table:         nil,    # used only for broker selection; omit for single-broker setups
  #     extra_headers: {}      # merged into every HTTP request
  #   )
  #
  # == Iteration
  #
  #   # Page-by-page (each page is a BrokerResponse):
  #   paginator.each_page { |resp| process(resp.result_table) }
  #
  #   # Row-by-row (each row is an Array of JsonNumber/String cells):
  #   paginator.each_row { |row| puts row.map(&:to_s).join(", ") }
  #
  #   # Enumerable methods work because #each is aliased to #each_row:
  #   rows = paginator.to_a
  #   paginator.select { |row| row.first.to_s.to_i > 100 }
  #
  # == Cursor lifecycle
  #
  # The cursor is deleted from the broker automatically after the last page is
  # consumed. Call #delete explicitly for early cleanup (e.g. break out of loop).
  # DELETE failures are swallowed — cursors expire naturally on the broker side.
  #
  # == Protocol
  #
  # 1. POST /query/sql?getCursor=true&numRows=N  — submit query, get first page + requestId
  # 2. GET  /responseStore/{id}/results?offset=K&numRows=N  — fetch subsequent pages
  # 3. DELETE /responseStore/{id}  — release cursor (best-effort)
  class Paginator
    include Enumerable

    DEFAULT_PAGE_SIZE = 1000

    def initialize(http_client, broker_address, query, page_size:, extra_headers: {})
      raise ArgumentError, "page_size must be a positive integer" unless page_size.is_a?(Integer) && page_size > 0

      @http_client    = http_client
      @broker_address = broker_address
      @query          = query
      @page_size      = page_size
      @extra_headers  = extra_headers

      @request_id  = nil
      @cursor_base = nil # "http://host:port" — set after first response
      @exhausted   = false
    end

    # Yields each page as a BrokerResponse. Returns an Enumerator without a block.
    def each_page
      return enum_for(:each_page) unless block_given?

      # Submit the query and get the first page + cursor metadata
      first = submit_cursor
      return if first.result_table.nil? || first.result_table.rows.empty?

      yield first

      fetched = first.num_rows || first.result_table.rows.size
      total   = first.num_rows_result_set || 0

      while fetched < total
        page = fetch_page(fetched)
        rows = page.result_table&.rows || []
        break if rows.empty?

        yield page

        fetched += rows.size
        break if rows.size < @page_size
      end

      delete
    end

    # Yields each row Array across all pages. Returns an Enumerator without a block.
    # Aliased as #each so Enumerable methods (.map, .select, .to_a, etc.) work.
    def each(&block)
      return enum_for(:each) unless block_given?

      each_page do |response|
        response.result_table.rows.each(&block)
      end
    end

    alias each_row each

    # Delete the cursor from the broker early (also called automatically after exhaustion).
    def delete
      return unless @request_id && @cursor_base

      url = "#{@cursor_base}/responseStore/#{@request_id}"
      @http_client.delete(url, headers: json_headers)
      @request_id = nil
    rescue StandardError
      # best-effort; cursor will expire naturally
    end

    private

    def submit_cursor
      base   = broker_base(@broker_address)
      url    = "#{base}/query/sql?getCursor=true&numRows=#{@page_size}"
      body   = JSON.generate("sql" => @query)
      resp   = @http_client.post(url, body: body, headers: json_headers)

      raise TransportError, "cursor submit returned HTTP #{resp.code}" unless resp.code.to_i == 200

      parsed = BrokerResponse.from_json(resp.body)

      @request_id  = parsed.request_id
      @cursor_base = broker_base_from_response(parsed) || base

      parsed
    end

    def fetch_page(offset)
      url  = "#{@cursor_base}/responseStore/#{@request_id}/results?offset=#{offset}&numRows=#{@page_size}"
      resp = @http_client.get(url, headers: json_headers)

      raise TransportError, "cursor fetch returned HTTP #{resp.code}" unless resp.code.to_i == 200

      BrokerResponse.from_json(resp.body)
    end

    def broker_base(address)
      return address if address.start_with?("http://", "https://")

      "http://#{address}"
    end

    def broker_base_from_response(resp)
      return nil unless resp.broker_host && resp.broker_port

      "http://#{resp.broker_host}:#{resp.broker_port}"
    end

    def json_headers
      { "Content-Type" => "application/json; charset=utf-8" }.merge(@extra_headers)
    end
  end
end
