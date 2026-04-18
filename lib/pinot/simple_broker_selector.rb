module Pinot
  class SimpleBrokerSelector
    include BrokerSelector

    def initialize(broker_list)
      @broker_list = broker_list.dup.freeze
    end

    def init
      raise BrokerNotFoundError, "no pre-configured broker lists" if @broker_list.empty?
    end

    def select_broker(_table)
      raise BrokerNotFoundError, "no pre-configured broker lists" if @broker_list.empty?
      @broker_list.sample
    end
  end
end
