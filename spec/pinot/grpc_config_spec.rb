require "spec_helper"
require "pinot/grpc_config"

RSpec.describe Pinot::GrpcConfig do
  describe "default values" do
    subject(:config) { described_class.new }

    it "defaults broker_list to empty array" do
      expect(config.broker_list).to eq([])
    end

    it "defaults tls_config to nil" do
      expect(config.tls_config).to be_nil
    end

    it "defaults extra_metadata to empty hash" do
      expect(config.extra_metadata).to eq({})
    end

    it "defaults timeout to nil" do
      expect(config.timeout).to be_nil
    end
  end

  describe "constructor arguments" do
    it "accepts broker_list" do
      config = described_class.new(broker_list: ["host1:8090", "host2:8090"])
      expect(config.broker_list).to eq(["host1:8090", "host2:8090"])
    end

    it "accepts tls_config" do
      tls = Pinot::TlsConfig.new(insecure_skip_verify: true)
      config = described_class.new(tls_config: tls)
      expect(config.tls_config).to eq(tls)
    end

    it "accepts extra_metadata" do
      config = described_class.new(extra_metadata: { "X-Custom" => "value" })
      expect(config.extra_metadata).to eq({ "X-Custom" => "value" })
    end

    it "accepts timeout" do
      config = described_class.new(timeout: 30)
      expect(config.timeout).to eq(30)
    end
  end

  describe "attribute setters" do
    subject(:config) { described_class.new }

    it "allows setting broker_list" do
      config.broker_list = ["newhost:8090"]
      expect(config.broker_list).to eq(["newhost:8090"])
    end

    it "allows setting tls_config" do
      tls = Pinot::TlsConfig.new
      config.tls_config = tls
      expect(config.tls_config).to eq(tls)
    end

    it "allows setting extra_metadata" do
      config.extra_metadata = { "Authorization" => "Bearer token" }
      expect(config.extra_metadata).to eq({ "Authorization" => "Bearer token" })
    end

    it "allows setting timeout" do
      config.timeout = 10.5
      expect(config.timeout).to eq(10.5)
    end
  end
end
