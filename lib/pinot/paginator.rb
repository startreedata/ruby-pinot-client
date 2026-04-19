module Pinot
  # Implements Pinot's server-side cursor API.
  #
  # The broker stores the full result set and returns slices on demand.
  # All fetch requests after the first must go to the same broker that
  # owns the cursor state (brokerHost:brokerPort from the initial response).
  #
  # Usage:
  #   paginator = conn.paginate("SELECT * FROM t LIMIT 10000", page_size: 100)
  #   paginator.each_page { |resp| process(resp.result_table) }
  #   paginator.each_row  { |row|  puts row.map(&:to_s).join(", ") }
  #   paginator.delete    # optional early cleanup; also called automatically on exhaustion
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
      @cursor_base = nil  # "http://host:port" — set after first response
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
