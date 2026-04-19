begin
  require "grpc"
  require_relative "proto/broker_service_pb"
  require_relative "proto/broker_service_services_pb"
rescue LoadError
  raise LoadError, "The 'grpc' gem is required to use Pinot::GrpcTransport. Add it to your Gemfile: gem \"grpc\""
end
require_relative "grpc_config"
require_relative "response"

module Pinot
  class GrpcTransport
    def initialize(grpc_config)
      @config = grpc_config
    end

    # Returns Pinot::BrokerResponse
    def execute(broker_address, request)
      stub = build_stub(broker_address)
      grpc_request = build_request(request)
      call_opts = build_call_opts(request)

      grpc_response = stub.submit(grpc_request, **call_opts)
      BrokerResponse.from_json(grpc_response.payload)
    rescue GRPC::BadStatus => e
      raise TransportError, "gRPC error #{e.code}: #{e.details}"
    end

    private

    def build_stub(broker_address)
      creds = if @config.tls_config
                build_ssl_credentials
              else
                :this_channel_is_insecure
              end
      Pinot::Broker::Grpc::PinotClientGrpcBrokerService::Stub.new(
        broker_address, creds
      )
    end

    def build_request(request)
      meta = {}
      query_opts = build_query_options(request)
      meta["queryOptions"] = query_opts unless query_opts.empty?
      meta["traceEnabled"] = "true" if request.trace
      meta.merge!(@config.extra_metadata)

      Pinot::Broker::Grpc::BrokerRequest.new(
        sql:      request.query,
        metadata: meta
      )
    end

    def build_query_options(request)
      parts = ["groupByMode=sql", "responseFormat=sql"]
      parts << "useMultistageEngine=true" if request.use_multistage_engine
      parts << "timeoutMs=#{request.query_timeout_ms}" if request.query_timeout_ms
      parts.join(";")
    end

    def build_call_opts(request)
      opts = {}
      # Per-request timeout takes precedence over config-level timeout
      timeout_s = if request.query_timeout_ms
                    request.query_timeout_ms / 1000.0
                  elsif @config.timeout
                    @config.timeout
                  end
      opts[:deadline] = Time.now + timeout_s if timeout_s
      opts[:metadata] = @config.extra_metadata unless @config.extra_metadata.empty?
      opts
    end

    def build_ssl_credentials
      tls = @config.tls_config
      root_certs = tls.ca_cert_file ? File.read(tls.ca_cert_file) : nil
      private_key = tls.client_key_file ? File.read(tls.client_key_file) : nil
      cert_chain  = tls.client_cert_file ? File.read(tls.client_cert_file) : nil
      GRPC::Core::ChannelCredentials.new(root_certs, private_key, cert_chain)
    end
  end
end
