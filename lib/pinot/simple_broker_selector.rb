module Pinot
  class SimpleBrokerSelector
    include BrokerSelector

    def initialize(broker_list)
      @broker_list = broker_list.dup.freeze
      @mutex = Mutex.new
      @index = 0
    end

    def init
      raise BrokerNotFoundError, "no pre-configured broker lists" if @broker_list.empty?
    end

    def select_broker(_table)
      raise BrokerNotFoundError, "no pre-configured broker lists" if @broker_list.empty?
      @mutex.synchronize do
        broker = @broker_list[@index % @broker_list.size]
        @index += 1
        broker
      end
    end
  end
end
