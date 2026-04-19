require "net/http"
require "uri"
require "json"
require "securerandom"
require "openssl"

module Pinot
  class HttpClient
    DEFAULT_POOL_SIZE = 5
    DEFAULT_KEEP_ALIVE_TIMEOUT = 30

    PoolEntry = Struct.new(:http, :checked_in_at)

    def initialize(timeout: nil, tls_config: nil, pool_size: nil, keep_alive_timeout: nil)
      @timeout = timeout
      @tls_config = tls_config
      @max_pool_size = pool_size || DEFAULT_POOL_SIZE
      @keep_alive_timeout = keep_alive_timeout || DEFAULT_KEEP_ALIVE_TIMEOUT
      @pool = {}
      @pool_mutex = Mutex.new
      @reaper = start_reaper
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

    def close
      @reaper.kill rescue nil
      @pool_mutex.synchronize do
        @pool.each_value do |entries|
          entries.each { |entry| entry.http.finish rescue nil }
        end
        @pool.clear
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
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      http = @pool_mutex.synchronize do
        entries = @pool[key] ||= []
        fresh = nil
        while (entry = entries.pop)
          if now - entry.checked_in_at < @keep_alive_timeout
            fresh = entry.http
            break
          else
            entry.http.finish rescue nil
          end
        end
        fresh
      end
      http || new_connection(uri)
    end

    def checkin(key, http)
      @pool_mutex.synchronize do
        pool_for_key = @pool[key] ||= []
        if pool_for_key.size < @max_pool_size
          pool_for_key.push(PoolEntry.new(http, Process.clock_gettime(Process::CLOCK_MONOTONIC)))
        else
          http.finish rescue nil
        end
      end
    end

    def start_reaper
      t = Thread.new do
        loop do
          sleep @keep_alive_timeout / 2.0
          reap_stale_connections
        end
      end
      t.abort_on_exception = false
      t
    end

    def reap_stale_connections
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @pool_mutex.synchronize do
        @pool.each_value do |entries|
          entries.reject! do |entry|
            if now - entry.checked_in_at >= @keep_alive_timeout
              entry.http.finish rescue nil
              true
            else
              false
            end
          end
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

    RETRYABLE_ERRORS = [
      Errno::ECONNRESET, Errno::ECONNREFUSED, Errno::ETIMEDOUT,
      Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout
    ].freeze

    # HTTP status codes that map to specific error classes and are safe to retry
    HTTP_ERROR_MAP = {
      "408" => QueryTimeoutError,
      "429" => RateLimitError,
      "503" => BrokerUnavailableError,
      "504" => BrokerUnavailableError
    }.freeze

    RETRYABLE_HTTP_ERRORS = [RateLimitError, BrokerUnavailableError].freeze

    # Pinot exception errorCode values that indicate query timeout.
    # 250 = ExecutionTimeoutError (server-side), 400 = BrokerTimeoutError.
    TIMEOUT_ERROR_CODES = [250, 400].freeze

    def initialize(http_client:, extra_headers: {}, timeout_ms: nil, logger: nil,
                   max_retries: 0, retry_interval_ms: 200)
      @http_client = http_client
      @extra_headers = extra_headers
      @timeout_ms = timeout_ms
      @logger = logger
      @max_retries = max_retries
      @retry_interval_ms = retry_interval_ms
    end

    def execute(broker_address, request, extra_request_headers: {})
      logger.debug "Pinot query to #{broker_address}: #{request.query}"

      attempts = 0
      max_attempts = (@max_retries || 0) + 1

      begin
        attempts += 1

        url = build_url(broker_address, request.query_format)
        body = build_body(request)
        headers = DEFAULT_HEADERS
          .merge(@extra_headers)
          .merge("X-Correlation-Id" => SecureRandom.uuid)
          .merge(extra_request_headers)

        resp = @http_client.post(url, body: body, headers: headers)

        if (error_class = HTTP_ERROR_MAP[resp.code])
          logger.error "Pinot broker returned HTTP #{resp.code}"
          raise error_class, "http exception with HTTP status code #{resp.code}"
        end

        unless resp.code.to_i == 200
          logger.error "Pinot broker returned HTTP #{resp.code}"
          raise TransportError, "http exception with HTTP status code #{resp.code}"
        end

        broker_response = begin
          BrokerResponse.from_json(resp.body)
        rescue JSON::ParserError => e
          raise e.message
        end

        if (timeout_ex = broker_response.exceptions.find { |ex| TIMEOUT_ERROR_CODES.include?(ex.error_code) })
          raise QueryTimeoutError, timeout_ex.message
        end

        broker_response
      rescue *RETRYABLE_HTTP_ERRORS, *RETRYABLE_ERRORS => e
        if attempts < max_attempts
          sleep_ms = (@retry_interval_ms || 200) * (2 ** (attempts - 1))
          sleep(sleep_ms / 1000.0)
          retry
        end
        raise Net::ReadTimeout === e || Net::WriteTimeout === e ? QueryTimeoutError.new(e.message) : e
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
