module Pinot
  # Build a Connection from a static list of broker addresses.
  #
  #   conn = Pinot.from_broker_list(["broker1:8099", "broker2:8099"])
  #
  # @param broker_list [Array<String>] broker host:port entries
  # @param http_client [HttpClient, nil] optional pre-configured HTTP client
  # @return [Connection]
  def self.from_broker_list(broker_list, http_client: nil)
    config = ClientConfig.new(broker_list: broker_list)
    from_config(config, http_client: http_client)
  end

  # Build a Connection backed by a Pinot controller for automatic broker discovery.
  # The controller is polled in the background to keep the broker list fresh.
  #
  #   conn = Pinot.from_controller("controller:9000")
  #
  # @param controller_address [String] controller host:port (or http://host:port)
  # @param http_client [HttpClient, nil] optional pre-configured HTTP client
  # @return [Connection]
  def self.from_controller(controller_address, http_client: nil)
    config = ClientConfig.new(
      controller_config: ControllerConfig.new(controller_address: controller_address)
    )
    from_config(config, http_client: http_client)
  end

  # Build a Connection from a fully specified ClientConfig.
  # This is the most flexible factory: it handles all transport types (HTTP,
  # gRPC, ZooKeeper) and wires up the circuit breaker and retry logic from
  # config flags.
  #
  #   config = Pinot::ClientConfig.new(
  #     broker_list:             ["broker:8099"],
  #     query_timeout_ms:        5_000,
  #     use_multistage_engine:   true,
  #     max_retries:             2,
  #     retry_interval_ms:       100,
  #     circuit_breaker_enabled: true
  #   )
  #   conn = Pinot.from_config(config)
  #
  # @param config     [ClientConfig] fully populated config object
  # @param http_client [HttpClient, nil] optional pre-configured HTTP client
  # @return [Connection]
  # @raise [ConfigurationError] if no broker source is specified in config
  def self.from_config(config, http_client: nil)
    config.validate!

    if config.grpc_config
      transport = GrpcTransport.new(config.grpc_config)
      selector  = SimpleBrokerSelector.new(config.grpc_config.broker_list)

      conn = Connection.new(
        transport: transport,
        broker_selector: selector,
        use_multistage_engine: config.use_multistage_engine || false,
        logger: config.logger,
        query_timeout_ms: config.query_timeout_ms
      )

      selector.init
      return conn
    end

    if config.zookeeper_config
      selector = ZookeeperBrokerSelector.new(zk_path: config.zookeeper_config.zk_path)
      selector.init

      inner = http_client || build_http_client(config)
      transport = JsonHttpTransport.new(
        http_client: inner,
        extra_headers: config.extra_http_header || {},
        logger: config.logger,
        timeout_ms: config.query_timeout_ms,
        max_retries: config.max_retries || 0,
        retry_interval_ms: config.retry_interval_ms || 200
      )

      return Connection.new(
        transport: transport,
        broker_selector: selector,
        use_multistage_engine: config.use_multistage_engine || false,
        logger: config.logger,
        query_timeout_ms: config.query_timeout_ms,
        circuit_breaker_registry: build_circuit_breaker_registry(config)
      )
    end

    inner = http_client || build_http_client(config)

    transport = JsonHttpTransport.new(
      http_client: inner,
      extra_headers: config.extra_http_header || {},
      logger: config.logger,
      max_retries: config.max_retries || 0,
      retry_interval_ms: config.retry_interval_ms || 200
    )

    selector = build_selector(config, inner)
    raise ConfigurationError, "must specify broker_list or controller_config" unless selector

    conn = Connection.new(
      transport: transport,
      broker_selector: selector,
      use_multistage_engine: config.use_multistage_engine || false,
      logger: config.logger,
      query_timeout_ms: config.query_timeout_ms,
      circuit_breaker_registry: build_circuit_breaker_registry(config)
    )

    selector.init
    conn
  end

  def self.build_http_client(config)
    HttpClient.new(
      timeout: config.http_timeout,
      tls_config: config.tls_config,
      pool_size: config.pool_size,
      keep_alive_timeout: config.keep_alive_timeout
    )
  end
  private_class_method :build_http_client

  def self.build_selector(config, http_client)
    if config.broker_list && !config.broker_list.empty?
      SimpleBrokerSelector.new(config.broker_list)
    elsif config.controller_config
      ControllerBasedBrokerSelector.new(config.controller_config, http_client, logger: config.logger)
    end
  end
  private_class_method :build_selector

  def self.build_circuit_breaker_registry(config)
    return nil unless config.circuit_breaker_enabled

    CircuitBreakerRegistry.new(
      failure_threshold: config.circuit_breaker_threshold || 5,
      open_timeout:      config.circuit_breaker_timeout   || 30
    )
  end
  private_class_method :build_circuit_breaker_registry
end
