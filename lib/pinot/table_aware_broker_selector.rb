module Pinot
  class TableAwareBrokerSelector
    include BrokerSelector

    OFFLINE_SUFFIX  = "_OFFLINE".freeze
    REALTIME_SUFFIX = "_REALTIME".freeze

    def initialize
      @mutex = Mutex.new
      @all_broker_list = []
      @table_broker_map = {}
    end

    def init
      raise NotImplementedError, "subclasses must implement init"
    end

    def select_broker(table)
      table_name = extract_table_name(table.to_s)
      @mutex.synchronize do
        if table_name.empty?
          raise BrokerNotFoundError, "no available broker" if @all_broker_list.empty?

          return @all_broker_list.sample
        end
        brokers = @table_broker_map[table_name]
        raise TableNotFoundError, "unable to find table: #{table}" unless brokers
        raise BrokerNotFoundError, "no available broker for table: #{table}" if brokers.empty?

        brokers.sample
      end
    end

    def update_broker_data(all_brokers, table_map)
      @mutex.synchronize do
        @all_broker_list = all_brokers
        @table_broker_map = table_map
      end
    end

    private

    def extract_table_name(table)
      table.sub(/#{OFFLINE_SUFFIX}\z/, "").sub(/#{REALTIME_SUFFIX}\z/, "")
    end
  end
end
