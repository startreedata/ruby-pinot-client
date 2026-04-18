require "net/http"
require "uri"
require "json"
require "securerandom"

module Pinot
  class HttpClient
    def initialize(timeout: nil)
      @timeout = timeout
    end

    def post(url, body:, headers: {})
      uri = URI.parse(url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == "https"
      if @timeout
        http.open_timeout = @timeout
        http.read_timeout = @timeout
        http.write_timeout = @timeout
      end
      req = Net::HTTP::Post.new(uri.request_uri)
      headers.each { |k, v| req[k] = v }
      req.body = body
      http.request(req)
    end

    def get(url, headers: {})
      uri = URI.parse(url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == "https"
      if @timeout
        http.open_timeout = @timeout
        http.read_timeout = @timeout
        http.write_timeout = @timeout
      end
      req = Net::HTTP::Get.new(uri.request_uri)
      headers.each { |k, v| req[k] = v }
      http.request(req)
    end
  end

  class JsonHttpTransport
    DEFAULT_HEADERS = {
      "Content-Type" => "application/json; charset=utf-8"
    }.freeze

    def initialize(http_client:, extra_headers: {}, timeout_ms: nil, logger: nil)
      @http_client = http_client
      @extra_headers = extra_headers
      @timeout_ms = timeout_ms
      @logger = logger
    end

    def execute(broker_address, request)
      logger.debug "Pinot query to #{broker_address}: #{request.query}"

      url = build_url(broker_address, request.query_format)
      body = build_body(request)
      headers = DEFAULT_HEADERS
        .merge(@extra_headers)
        .merge("X-Correlation-Id" => SecureRandom.uuid)

      resp = @http_client.post(url, body: body, headers: headers)

      unless resp.code.to_i == 200
        logger.error "Pinot broker returned HTTP #{resp.code}"
        raise TransportError, "http exception with HTTP status code #{resp.code}"
      end

      begin
        BrokerResponse.from_json(resp.body)
      rescue JSON::ParserError => e
        raise e.message
      end
    end

    private

    def logger
      @logger || Pinot::Logging.logger
    end

    def build_url(broker_address, query_format)
      base = if broker_address.start_with?("http://", "https://")
               broker_address
             else
               "http://#{broker_address}"
             end
      path = query_format == "sql" ? "/query/sql" : "/query"
      "#{base}#{path}"
    end

    def build_body(request)
      payload = { request.query_format => request.query }
      opts = build_query_options(request)
      payload["queryOptions"] = opts unless opts.empty?
      payload["trace"] = "true" if request.trace
      JSON.generate(payload)
    end

    def build_query_options(request)
      parts = []
      if request.query_format == "sql"
        parts << "groupByMode=sql;responseFormat=sql"
        parts << "useMultistageEngine=true" if request.use_multistage_engine
        if @timeout_ms && @timeout_ms > 0
          parts << "timeoutMs=#{@timeout_ms}"
        end
      end
      parts.join(";")
    end
  end
end
