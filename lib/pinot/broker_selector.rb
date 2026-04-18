module Pinot
  module BrokerSelector
    # Implementers must define:
    #   def init        -> nil or raise on failure
    #   def select_broker(table) -> String "host:port" or raise
  end
end
