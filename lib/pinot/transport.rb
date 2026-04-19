require "net/http"
require "uri"
require "json"
require "securerandom"
require "openssl"

module Pinot
  class HttpClient
    MAX_POOL_SIZE = 5
    KEEP_ALIVE_TIMEOUT = 30

    def initialize(timeout: nil, tls_config: nil)
      @timeout = timeout
      @tls_config = tls_config
      @pool = {}
      @pool_mutex = Mutex.new
    end

    def post(url, body:, headers: {})
      uri = URI.parse(url)
      with_connection(url) do |http|
        req = Net::HTTP::Post.new(uri.request_uri)
        headers.each { |k, v| req[k] = v }
        req.body = body
        http.request(req)
      end
    end

    def get(url, headers: {})
      uri = URI.parse(url)
      with_connection(url) do |http|
        req = Net::HTTP::Get.new(uri.request_uri)
        headers.each { |k, v| req[k] = v }
        http.request(req)
      end
    end

    private

    def with_connection(url)
      uri = URI.parse(url)
      key = "#{uri.host}:#{uri.port}"
      http = checkout(key, uri)
      begin
        result = yield http
        checkin(key, http)
        result
      rescue => e
        http.finish rescue nil
        raise e
      end
    end

    def checkout(key, uri)
      @pool_mutex.synchronize { @pool[key]&.pop } || new_connection(uri)
    end

    def checkin(key, http)
      @pool_mutex.synchronize do
        pool_for_key = @pool[key] ||= []
        if pool_for_key.size < MAX_POOL_SIZE
          pool_for_key.push(http)
        else
          http.finish rescue nil
        end
      end
    end

    def new_connection(uri)
      http = Net::HTTP.new(uri.host, uri.port)
      configure_ssl(http, uri)
      if @timeout
        http.open_timeout = @timeout
        http.read_timeout = @timeout
        http.write_timeout = @timeout
      end
      http.start
      http
    end

    def configure_ssl(http, uri)
      if uri.scheme == "https"
        http.use_ssl = true
        if @tls_config
          if @tls_config.ca_cert_file
            store = OpenSSL::X509::Store.new
            store.add_file(@tls_config.ca_cert_file)
            http.cert_store = store
          end
          if @tls_config.client_cert_file && @tls_config.client_key_file
            http.cert = OpenSSL::X509::Certificate.new(File.read(@tls_config.client_cert_file))
            http.key = OpenSSL::PKey.read(File.read(@tls_config.client_key_file))
          end
          if @tls_config.insecure_skip_verify
            http.verify_mode = OpenSSL::SSL::VERIFY_NONE
          else
            http.verify_mode = OpenSSL::SSL::VERIFY_PEER
          end
        end
      else
        http.use_ssl = false
      end
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
        parts << "timeoutMs=#{request.query_timeout_ms}" if request.query_timeout_ms
      end
      parts.join(";")
    end
  end
end
