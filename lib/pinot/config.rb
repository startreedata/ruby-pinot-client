module Pinot
  class ControllerConfig
    attr_accessor :controller_address, :update_freq_ms, :extra_controller_api_headers

    def initialize(controller_address: nil, update_freq_ms: 1000, extra_controller_api_headers: {})
      @controller_address = controller_address
      @update_freq_ms = update_freq_ms
      @extra_controller_api_headers = extra_controller_api_headers
    end
  end

  class ClientConfig
    attr_accessor :broker_list, :http_timeout, :extra_http_header,
                  :use_multistage_engine, :controller_config, :logger

    def initialize(
      broker_list: [],
      http_timeout: nil,
      extra_http_header: {},
      use_multistage_engine: false,
      controller_config: nil,
      logger: nil
    )
      @broker_list = broker_list
      @http_timeout = http_timeout
      @extra_http_header = extra_http_header
      @use_multistage_engine = use_multistage_engine
      @controller_config = controller_config
      @logger = logger
    end
  end
end
