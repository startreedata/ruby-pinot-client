module Pinot
  class Error < StandardError; end
  class BrokerNotFoundError < Error; end
  class TableNotFoundError < Error; end
  class TransportError < Error; end
  class PreparedStatementClosedError < Error; end
  class ConfigurationError < Error; end
end
