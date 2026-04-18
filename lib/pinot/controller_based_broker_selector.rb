require "net/http"
require "uri"
require "json"

module Pinot
  class ControllerBasedBrokerSelector < TableAwareBrokerSelector
    CONTROLLER_API_PATH = "/v2/brokers/tables?state=ONLINE"
    DEFAULT_UPDATE_FREQ_MS = 1000

    def initialize(config, http_client = nil)
      super()
      @config = config
      @internal_http = http_client || HttpClient.new
    end

    def init
      @config.update_freq_ms ||= DEFAULT_UPDATE_FREQ_MS
      @controller_url = build_controller_url(@config.controller_address)
      fetch_and_update
      start_background_refresh
      nil
    end

    def build_controller_url(address)
      addr = address.to_s
      if addr.include?("://")
        scheme = addr.split("://").first
        unless %w[http https].include?(scheme)
          raise "unsupported controller URL scheme: #{scheme}"
        end
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

      unless resp.code.to_i == 200
        raise "controller API returned HTTP status code #{resp.code}"
      end

      body = resp.body
      begin
        raw = JSON.parse(body)
      rescue JSON::ParserError => e
        raise "error decoding controller API response: #{e.message}"
      end

      cr = ControllerResponse.new(raw)
      update_broker_data(cr.extract_broker_list, cr.extract_table_to_broker_map)
    end

    def start_background_refresh
      interval = @config.update_freq_ms / 1000.0
      Thread.new do
        loop do
          sleep interval
          begin
            fetch_and_update
          rescue => e
            warn "Pinot: error refreshing broker data: #{e.message}"
          end
        end
      end
    end
  end
end
