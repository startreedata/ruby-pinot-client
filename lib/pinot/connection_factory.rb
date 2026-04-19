module Pinot
  def self.from_broker_list(broker_list, http_client: nil)
    config = ClientConfig.new(broker_list: broker_list)
    from_config(config, http_client: http_client)
  end

  def self.from_controller(controller_address, http_client: nil)
    config = ClientConfig.new(
      controller_config: ControllerConfig.new(controller_address: controller_address)
    )
    from_config(config, http_client: http_client)
  end

  def self.from_config(config, http_client: nil)
    if config.grpc_config
      transport = GrpcTransport.new(config.grpc_config)
      selector  = SimpleBrokerSelector.new(config.grpc_config.broker_list)

      conn = Connection.new(
        transport: transport,
        broker_selector: selector,
        use_multistage_engine: config.use_multistage_engine || false,
        logger: config.logger
      )

      selector.init
      return conn
    end

    if config.zookeeper_config
      selector = ZookeeperBrokerSelector.new(zk_path: config.zookeeper_config.zk_path)
      selector.init

      inner = http_client || HttpClient.new(timeout: config.http_timeout, tls_config: config.tls_config)
      transport = JsonHttpTransport.new(
        http_client: inner,
        extra_headers: config.extra_http_header || {},
        logger: config.logger
      )

      return Connection.new(
        transport: transport,
        broker_selector: selector,
        use_multistage_engine: config.use_multistage_engine || false,
        logger: config.logger
      )
    end

    inner = http_client || HttpClient.new(timeout: config.http_timeout, tls_config: config.tls_config)

    transport = JsonHttpTransport.new(
      http_client: inner,
      extra_headers: config.extra_http_header || {},
      logger: config.logger
    )

    selector = build_selector(config, inner)
    raise ConfigurationError, "must specify broker_list or controller_config" unless selector

    conn = Connection.new(
      transport: transport,
      broker_selector: selector,
      use_multistage_engine: config.use_multistage_engine || false,
      logger: config.logger,
      query_timeout_ms: config.query_timeout_ms
    )

    selector.init
    conn
  end

  def self.build_selector(config, http_client)
    if config.broker_list && !config.broker_list.empty?
      SimpleBrokerSelector.new(config.broker_list)
    elsif config.controller_config
      ControllerBasedBrokerSelector.new(config.controller_config, http_client, logger: config.logger)
    end
  end
  private_class_method :build_selector
end
