module Pinot
  Request = Struct.new(:query_format, :query, :trace, :use_multistage_engine)
end
