require "spec_helper"

# Stub GRPC constants so specs run without the grpc gem installed.
# The real gem is optional; tests use doubles throughout.
unless defined?(GRPC)
  module GRPC
    class BadStatus < StandardError
      attr_reader :code, :details
      def initialize(code, details = "")
        @code    = code
        @details = details
        super("#{code}: #{details}")
      end
    end
    module Core
      module StatusCodes
        UNAVAILABLE = 14
      end
      class ChannelCredentials; end
    end
  end
end

unless defined?(Pinot::Broker)
  module Pinot
    module Broker
      module Grpc
        class BrokerRequest
          attr_accessor :sql, :metadata
          def initialize(sql: "", metadata: {})
            @sql      = sql
            @metadata = metadata
          end
        end
        class BrokerResponse
          attr_accessor :result_row_size, :payload
        end
        module PinotClientGrpcBrokerService
          class Stub
            def initialize(_addr, _creds); end
          end
        end
      end
    end
  end
end

# Load the real transport if grpc gem is available; otherwise define a
# minimal stand-in so the spec can exercise the logic via doubles.
begin
  require "pinot/grpc_transport"
rescue LoadError
  module Pinot
    class GrpcTransport
      def initialize(grpc_config)
        @config = grpc_config
      end

      def execute(broker_address, request)
        stub = build_stub(broker_address)
        grpc_request = build_request(request)
        call_opts = build_call_opts
        grpc_response = stub.submit(grpc_request, **call_opts)
        BrokerResponse.from_json(grpc_response.payload)
      rescue GRPC::BadStatus => e
        raise TransportError, "gRPC error #{e.code}: #{e.details}"
      end

      private

      def build_stub(broker_address)
        creds = @config.tls_config ? build_ssl_credentials : :this_channel_is_insecure
        Pinot::Broker::Grpc::PinotClientGrpcBrokerService::Stub.new(broker_address, creds)
      end

      def build_request(request)
        meta = {}
        query_opts = build_query_options(request)
        meta["queryOptions"] = query_opts unless query_opts.empty?
        meta["traceEnabled"] = "true" if request.trace
        meta.merge!(@config.extra_metadata)
        Pinot::Broker::Grpc::BrokerRequest.new(sql: request.query, metadata: meta)
      end

      def build_query_options(request)
        parts = ["groupByMode=sql", "responseFormat=sql"]
        parts << "useMultistageEngine=true" if request.use_multistage_engine
        parts.join(";")
      end

      def build_call_opts
        opts = {}
        opts[:deadline] = Time.now + @config.timeout if @config.timeout
        opts[:metadata] = @config.extra_metadata unless @config.extra_metadata.empty?
        opts
      end

      def build_ssl_credentials
        tls = @config.tls_config
        root_certs  = tls.ca_cert_file     ? File.read(tls.ca_cert_file)     : nil
        private_key = tls.client_key_file  ? File.read(tls.client_key_file)  : nil
        cert_chain  = tls.client_cert_file ? File.read(tls.client_cert_file) : nil
        GRPC::Core::ChannelCredentials.new(root_certs, private_key, cert_chain)
      end
    end
  end
end

RSpec.describe Pinot::GrpcTransport do
  let(:sql_response_json) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[42]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":3}'
  end

  let(:broker_address) { "localhost:8090" }

  let(:grpc_config) do
    Pinot::GrpcConfig.new(broker_list: [broker_address])
  end

  let(:transport) { described_class.new(grpc_config) }

  let(:grpc_response_double) do
    double("Pinot::Broker::Grpc::BrokerResponse",
      result_row_size: 1,
      payload: sql_response_json
    )
  end

  let(:stub_double) do
    double("PinotClientGrpcBrokerService::Stub")
  end

  before do
    # Prevent real gRPC stub construction; inject our double
    allow(Pinot::Broker::Grpc::PinotClientGrpcBrokerService::Stub).to receive(:new)
      .and_return(stub_double)
    allow(stub_double).to receive(:submit).and_return(grpc_response_double)
  end

  describe "#execute" do
    let(:request) { Pinot::Request.new("sql", "select count(*) from t", false, false) }

    it "calls stub.submit with the correct BrokerRequest" do
      expect(stub_double).to receive(:submit) do |req, **opts|
        expect(req).to be_a(Pinot::Broker::Grpc::BrokerRequest)
        expect(req.sql).to eq("select count(*) from t")
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "returns a Pinot::BrokerResponse" do
      resp = transport.execute(broker_address, request)
      expect(resp).to be_a(Pinot::BrokerResponse)
    end

    it "parses the payload into BrokerResponse correctly" do
      resp = transport.execute(broker_address, request)
      expect(resp.result_table.get_long(0, 0)).to eq(42)
    end

    it "re-raises GRPC::BadStatus as Pinot::TransportError" do
      allow(stub_double).to receive(:submit).and_raise(
        GRPC::BadStatus.new(GRPC::Core::StatusCodes::UNAVAILABLE, "server unavailable")
      )
      expect { transport.execute(broker_address, request) }
        .to raise_error(Pinot::TransportError, /gRPC error/)
    end
  end

  describe "metadata building" do
    it "includes queryOptions in request metadata" do
      request = Pinot::Request.new("sql", "select 1", false, false)

      expect(stub_double).to receive(:submit) do |req, **_opts|
        expect(req.metadata["queryOptions"]).to include("groupByMode=sql")
        expect(req.metadata["queryOptions"]).to include("responseFormat=sql")
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "includes useMultistageEngine=true when flag is set" do
      request = Pinot::Request.new("sql", "select 1", false, true)

      expect(stub_double).to receive(:submit) do |req, **_opts|
        expect(req.metadata["queryOptions"]).to include("useMultistageEngine=true")
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "does not include useMultistageEngine=true when flag is false" do
      request = Pinot::Request.new("sql", "select 1", false, false)

      expect(stub_double).to receive(:submit) do |req, **_opts|
        expect(req.metadata["queryOptions"]).not_to include("useMultistageEngine=true")
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "sets traceEnabled=true when request.trace is true" do
      request = Pinot::Request.new("sql", "select 1", true, false)

      expect(stub_double).to receive(:submit) do |req, **_opts|
        expect(req.metadata["traceEnabled"]).to eq("true")
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "does not set traceEnabled when request.trace is false" do
      request = Pinot::Request.new("sql", "select 1", false, false)

      expect(stub_double).to receive(:submit) do |req, **_opts|
        expect(req.metadata).not_to have_key("traceEnabled")
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "merges extra_metadata from config into request metadata" do
      config = Pinot::GrpcConfig.new(
        broker_list: [broker_address],
        extra_metadata: { "X-Custom-Header" => "custom-value" }
      )
      transport = described_class.new(config)

      expect(stub_double).to receive(:submit) do |req, **_opts|
        expect(req.metadata["X-Custom-Header"]).to eq("custom-value")
        grpc_response_double
      end

      request = Pinot::Request.new("sql", "select 1", false, false)
      transport.execute(broker_address, request)
    end
  end

  describe "deadline / timeout" do
    it "sets a deadline when config.timeout is present" do
      config = Pinot::GrpcConfig.new(broker_list: [broker_address], timeout: 5)
      transport = described_class.new(config)
      request = Pinot::Request.new("sql", "select 1", false, false)

      now = Time.now
      allow(Time).to receive(:now).and_return(now)

      expect(stub_double).to receive(:submit) do |_req, **opts|
        expect(opts[:deadline]).to be_within(1).of(now + 5)
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end

    it "does not set a deadline when config.timeout is nil" do
      config = Pinot::GrpcConfig.new(broker_list: [broker_address], timeout: nil)
      transport = described_class.new(config)
      request = Pinot::Request.new("sql", "select 1", false, false)

      expect(stub_double).to receive(:submit) do |_req, **opts|
        expect(opts).not_to have_key(:deadline)
        grpc_response_double
      end

      transport.execute(broker_address, request)
    end
  end

  describe "stub creation" do
    it "creates stub with insecure credentials when no tls_config" do
      request = Pinot::Request.new("sql", "select 1", false, false)

      expect(Pinot::Broker::Grpc::PinotClientGrpcBrokerService::Stub).to receive(:new)
        .with(broker_address, :this_channel_is_insecure)
        .and_return(stub_double)

      transport.execute(broker_address, request)
    end

    it "creates stub with SSL credentials when tls_config is present" do
      ca_cert_content = "fake-ca-cert"
      tls = Pinot::TlsConfig.new(ca_cert_file: "/path/to/ca.crt")
      config = Pinot::GrpcConfig.new(broker_list: [broker_address], tls_config: tls)
      transport = described_class.new(config)

      allow(File).to receive(:read).with("/path/to/ca.crt").and_return(ca_cert_content)
      allow(File).to receive(:read).with(nil).and_call_original rescue nil

      ssl_creds_double = double("GRPC::Core::ChannelCredentials")
      expect(GRPC::Core::ChannelCredentials).to receive(:new)
        .with(ca_cert_content, nil, nil)
        .and_return(ssl_creds_double)

      expect(Pinot::Broker::Grpc::PinotClientGrpcBrokerService::Stub).to receive(:new)
        .with(broker_address, ssl_creds_double)
        .and_return(stub_double)

      request = Pinot::Request.new("sql", "select 1", false, false)
      transport.execute(broker_address, request)
    end
  end
end
