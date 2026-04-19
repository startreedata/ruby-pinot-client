module Pinot
  Request = Struct.new(:query_format, :query, :trace, :use_multistage_engine, :query_timeout_ms)
end
