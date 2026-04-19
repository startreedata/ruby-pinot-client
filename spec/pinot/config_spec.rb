RSpec.describe Pinot::ClientConfig do
  describe "#validate!" do
    it "passes with broker_list set" do
      config = Pinot::ClientConfig.new(broker_list: ["localhost:8000"])
      expect { config.validate! }.not_to raise_error
    end

    it "passes with controller_config set" do
      config = Pinot::ClientConfig.new(
        controller_config: Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      )
      expect { config.validate! }.not_to raise_error
    end

    it "raises ConfigurationError when nothing set" do
      config = Pinot::ClientConfig.new
      expect { config.validate! }.to raise_error(
        Pinot::ConfigurationError,
        /ClientConfig requires at least one of/
      )
    end

    it "raises ConfigurationError for negative http_timeout" do
      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8000"],
        http_timeout: -1
      )
      expect { config.validate! }.to raise_error(
        Pinot::ConfigurationError,
        /http_timeout must be positive, got: -1/
      )
    end

    it "raises ConfigurationError for zero query_timeout_ms" do
      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8000"],
        query_timeout_ms: 0
      )
      expect { config.validate! }.to raise_error(
        Pinot::ConfigurationError,
        /query_timeout_ms must be positive, got: 0/
      )
    end

    it "returns self on success (chainable)" do
      config = Pinot::ClientConfig.new(broker_list: ["localhost:8000"])
      expect(config.validate!).to equal(config)
    end
  end
end
