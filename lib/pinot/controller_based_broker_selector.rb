require "net/http"
require "uri"
require "json"

module Pinot
  class ControllerBasedBrokerSelector < TableAwareBrokerSelector
    CONTROLLER_API_PATH = "/v2/brokers/tables?state=ONLINE".freeze
    DEFAULT_UPDATE_FREQ_MS = 1000

    def initialize(config, http_client = nil, logger: nil)
      super()
      @config = config
      @internal_http = http_client || HttpClient.new
      @logger = logger
    end

    def init
      @config.update_freq_ms ||= DEFAULT_UPDATE_FREQ_MS
      @controller_url = build_controller_url(@config.controller_address)
      fetch_and_update
      logger.info "ControllerBasedBrokerSelector initialized with #{@all_broker_list.size} brokers"
      start_background_refresh
      nil
    end

    def build_controller_url(address)
      addr = address.to_s
      if addr.include?("://")
        scheme = addr.split("://").first
        raise ConfigurationError, "unsupported controller URL scheme: #{scheme}" unless %w[http https].include?(scheme)

        addr.chomp("/") + CONTROLLER_API_PATH
      else
        "http://#{addr.chomp("/")}#{CONTROLLER_API_PATH}"
      end
    end

    private

    def fetch_and_update
      headers = { "Accept" => "application/json" }
                  .merge(@config.extra_controller_api_headers || {})

      resp = @internal_http.get(@controller_url, headers: headers)

      raise TransportError, "controller API returned HTTP status code #{resp.code}" unless resp.code.to_i == 200

      body = resp.body
      begin
        raw = JSON.parse(body)
      rescue JSON::ParserError => e
        raise ConfigurationError, "error decoding controller API response: #{e.message}"
      end

      cr = ControllerResponse.new(raw)
      update_broker_data(cr.extract_broker_list, cr.extract_table_to_broker_map)
    end

    def logger
      @logger || Pinot::Logging.logger
    end

    def start_background_refresh
      interval = @config.update_freq_ms / 1000.0
      Thread.new do
        loop do
          sleep interval
          begin
            fetch_and_update
          rescue StandardError => e
            logger.warn "Pinot controller refresh failed: #{e.message}"
          end
        end
      end
    end
  end
end
