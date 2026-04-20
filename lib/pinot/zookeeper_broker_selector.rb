require "json"
require_relative "table_aware_broker_selector"
require_relative "errors"

module Pinot
  class ZookeeperBrokerSelector < TableAwareBrokerSelector
    # ZK path where Pinot stores broker external view
    BROKER_EXTERNAL_VIEW_PATH = "/EXTERNALVIEW/brokerResource".freeze

    def initialize(zk_path:, zk_client: nil)
      super()
      @zk_path   = zk_path      # e.g. "localhost:2181"
      @zk_client = zk_client    # injectable for testing
    end

    def init
      @zk = @zk_client || build_zk_client
      fetch_and_update
      setup_watcher
    end

    private

    def build_zk_client
      begin
        require "zk"
      rescue LoadError
        raise ConfigurationError, "The 'zk' gem is required to use ZookeeperBrokerSelector. Add it to your Gemfile: gem \"zk\""
      end
      ZK.new(@zk_path)
    end

    def fetch_and_update
      data, _stat = @zk.get(BROKER_EXTERNAL_VIEW_PATH)
      parsed = JSON.parse(data)
      all_brokers, table_map = parse_external_view(parsed)
      update_broker_data(all_brokers, table_map)
    end

    def setup_watcher
      @zk.register(BROKER_EXTERNAL_VIEW_PATH) do |_event|
        begin
          fetch_and_update
        rescue StandardError
          nil
        end
        setup_watcher # re-register watch after each trigger
      end
      # Set initial watch
      @zk.exists?(BROKER_EXTERNAL_VIEW_PATH, watch: true)
    end

    def parse_external_view(data)
      # Pinot external view format:
      # { "mapFields": { "tableName_OFFLINE": { "Broker_host_port": "ONLINE" } } }
      map_fields = data["mapFields"] || {}

      all_brokers = Set.new
      table_map   = {}

      map_fields.each do |raw_table, broker_map|
        # Strip _OFFLINE / _REALTIME suffix from the table key, matching Go's extractTableName
        table_name = raw_table.sub(/_OFFLINE\z/, "").sub(/_REALTIME\z/, "")
        brokers = []
        broker_map.each do |broker_key, state|
          next unless state == "ONLINE"

          # Broker key format: Broker_<hostname>_<port>
          # Use the last segment as port and the second-to-last as host
          parts = broker_key.split("_")
          next if parts.length < 2

          port = parts.last
          next unless port =~ /\A\d+\z/

          host = parts[-2]
          brokers << "#{host}:#{port}"
        end
        table_map[table_name] = brokers
        all_brokers.merge(brokers)
      end

      [all_brokers.to_a, table_map]
    end
  end
end
