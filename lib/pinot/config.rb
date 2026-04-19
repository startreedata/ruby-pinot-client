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
    attr_accessor :broker_list, :http_timeout, :query_timeout_ms, :extra_http_header,
                  :use_multistage_engine, :controller_config, :logger, :tls_config,
                  :grpc_config, :zookeeper_config,
                  :max_retries,        # Integer, default 0 (no retry)
                  :retry_interval_ms,  # Integer ms base interval, default 200
                  :pool_size,          # max idle connections per broker, default 5
                  :keep_alive_timeout  # seconds before idle connection is reaped, default 30

    def initialize(
      broker_list: [],
      http_timeout: nil,
      query_timeout_ms: nil,
      extra_http_header: {},
      use_multistage_engine: false,
      controller_config: nil,
      logger: nil,
      tls_config: nil,
      grpc_config: nil,
      zookeeper_config: nil,
      max_retries: 0,
      retry_interval_ms: 200,
      pool_size: nil,
      keep_alive_timeout: nil
    )
      @broker_list = broker_list
      @http_timeout = http_timeout
      @query_timeout_ms = query_timeout_ms
      @extra_http_header = extra_http_header
      @use_multistage_engine = use_multistage_engine
      @controller_config = controller_config
      @logger = logger
      @tls_config = tls_config
      @grpc_config = grpc_config
      @zookeeper_config = zookeeper_config
      @max_retries = max_retries
      @retry_interval_ms = retry_interval_ms
      @pool_size = pool_size
      @keep_alive_timeout = keep_alive_timeout
    end

    def validate!
      sources = [
        !broker_list.empty?,
        !controller_config.nil?,
        !zookeeper_config.nil?,
        !grpc_config.nil?
      ].count(true)

      if sources == 0
        raise ConfigurationError, "ClientConfig requires at least one of: broker_list, controller_config, zookeeper_config, or grpc_config"
      end

      if !http_timeout.nil? && http_timeout <= 0
        raise ConfigurationError, "http_timeout must be positive, got: #{http_timeout}"
      end

      if !query_timeout_ms.nil? && query_timeout_ms <= 0
        raise ConfigurationError, "query_timeout_ms must be positive, got: #{query_timeout_ms}"
      end

      if !pool_size.nil? && pool_size < 1
        raise ConfigurationError, "pool_size must be at least 1, got: #{pool_size}"
      end

      if !keep_alive_timeout.nil? && keep_alive_timeout <= 0
        raise ConfigurationError, "keep_alive_timeout must be positive, got: #{keep_alive_timeout}"
      end

      self
    end
  end
end
