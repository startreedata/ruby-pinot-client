require "net/http"
require "uri"
require "json"
require "bigdecimal"
require "securerandom"

require_relative "pinot/errors"
require_relative "pinot/instrumentation"
require_relative "pinot/version"
require_relative "pinot/logger"
require_relative "pinot/config"
require_relative "pinot/tls_config"
require_relative "pinot/request"
require_relative "pinot/response"
require_relative "pinot/broker_selector"
require_relative "pinot/simple_broker_selector"
require_relative "pinot/table_aware_broker_selector"
require_relative "pinot/controller_response"
require_relative "pinot/controller_based_broker_selector"
require_relative "pinot/transport"
require_relative "pinot/circuit_breaker"
require_relative "pinot/query_result"
require_relative "pinot/connection"
require_relative "pinot/prepared_statement"
require_relative "pinot/connection_factory"

require_relative "pinot/grpc_config"
begin
  require_relative "pinot/grpc_transport"
rescue LoadError
  # grpc gem not available; GrpcTransport disabled
end

begin
  require_relative "pinot/zookeeper_broker_selector"
rescue LoadError
end
