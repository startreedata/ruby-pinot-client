module Pinot
  class GrpcConfig
    attr_accessor :broker_list,       # Array<String> of "host:port" (no scheme)
                  :tls_config,        # Pinot::TlsConfig or nil
                  :extra_metadata,    # Hash — extra gRPC metadata headers
                  :timeout            # seconds (Integer/Float), nil = no timeout

    def initialize(broker_list: [], tls_config: nil, extra_metadata: {}, timeout: nil)
      @broker_list    = broker_list
      @tls_config     = tls_config
      @extra_metadata = extra_metadata
      @timeout        = timeout
    end
  end
end
