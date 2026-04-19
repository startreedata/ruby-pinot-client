module Pinot
  class Error < StandardError; end
  class BrokerNotFoundError < Error; end
  class TableNotFoundError < Error; end
  class TransportError < Error; end
  class BrokerUnavailableError < TransportError; end
  class QueryTimeoutError < TransportError; end
  class RateLimitError < TransportError; end
  class PreparedStatementClosedError < Error; end
  class ConfigurationError < Error; end
end
