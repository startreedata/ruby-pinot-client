require "spec_helper"

RSpec.describe Pinot::Logging do
  before do
    # Reset the global logger between tests
    described_class.logger = nil
  end

  after do
    described_class.logger = nil
  end

  describe ".logger" do
    it "returns a Logger instance by default" do
      expect(described_class.logger).to be_a(Logger)
    end

    it "default level is WARN" do
      expect(described_class.logger.level).to eq(Logger::WARN)
    end

    it "returns the same instance on repeated calls" do
      l1 = described_class.logger
      l2 = described_class.logger
      expect(l1).to equal(l2)
    end
  end

  describe ".logger=" do
    it "persists a custom logger" do
      custom = Logger.new(File::NULL)
      described_class.logger = custom
      expect(described_class.logger).to equal(custom)
    end

    it "a custom logger passed via ClientConfig is used instead of the global one" do
      custom = Logger.new(File::NULL)
      custom.level = Logger::DEBUG

      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8099"],
        logger: custom
      )
      expect(config.logger).to equal(custom)

      # Verify the transport created from config uses the custom logger
      transport = Pinot::JsonHttpTransport.new(
        http_client: Pinot::HttpClient.new,
        logger: config.logger
      )
      # The transport's private logger method returns the custom logger
      expect(transport.send(:logger)).to equal(custom)
    end
  end
end
