module Pinot
  class BrokerDto
    attr_reader :host, :port, :instance_name

    def initialize(hash)
      @host = hash["host"]
      @port = hash["port"].to_i
      @instance_name = hash["instanceName"]
    end

    def broker_address
      "#{@host}:#{@port}"
    end
  end

  class ControllerResponse
    def initialize(raw_hash)
      @data = raw_hash.transform_values do |brokers|
        brokers.map { |b| BrokerDto.new(b) }
      end
    end

    def extract_broker_list
      @data.values.flatten.map(&:broker_address).uniq
    end

    def extract_table_to_broker_map
      @data.transform_values { |brokers| brokers.map(&:broker_address) }
    end
  end
end
