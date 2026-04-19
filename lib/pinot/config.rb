module Pinot
  class ZookeeperConfig
    attr_accessor :zk_path,           # "host:port" or "host1:port,host2:port/chroot"
                  :session_timeout_ms # default 30000

    def initialize(zk_path:, session_timeout_ms: 30_000)
      @zk_path            = zk_path
      @session_timeout_ms = session_timeout_ms
    end
  end

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
                  :use_multistage_engine, :controller_config, :logger, :tls_config,
                  :grpc_config, :zookeeper_config, :query_timeout_ms

    def initialize(
      broker_list: [],
      http_timeout: nil,
      extra_http_header: {},
      use_multistage_engine: false,
      controller_config: nil,
      logger: nil,
      tls_config: nil,
      grpc_config: nil,
      zookeeper_config: nil,
      query_timeout_ms: nil
    )
      @broker_list = broker_list
      @http_timeout = http_timeout
      @extra_http_header = extra_http_header
      @use_multistage_engine = use_multistage_engine
      @controller_config = controller_config
      @logger = logger
      @tls_config = tls_config
      @grpc_config = grpc_config
      @zookeeper_config = zookeeper_config
      @query_timeout_ms = query_timeout_ms
    end
  end
end
