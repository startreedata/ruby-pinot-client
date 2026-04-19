RSpec.describe Pinot::ControllerBasedBrokerSelector do
  let(:broker_response) do
    '{"baseballStats":[{"port":8000,"host":"host1","instanceName":"Broker_host1_8000"}]}'
  end

  describe "#init" do
    it "populates broker data on success" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 200, body: broker_response, headers: { "Content-Type" => "application/json" })

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      sel.init

      broker = sel.select_broker("baseballStats")
      expect(broker).to eq "host1:8000"
    end

    it "sets default update_freq_ms to 1000 when nil" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 200, body: "{}", headers: {})

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000", update_freq_ms: nil)
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      sel.init

      expect(config.update_freq_ms).to eq 1000
    end

    it "raises on HTTP 500" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 500, body: "")

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      expect { sel.init }.to raise_error(Pinot::TransportError, /500/)
    end

    it "raises on network error" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_raise(Errno::ECONNREFUSED)

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      expect { sel.init }.to raise_error(Errno::ECONNREFUSED)
    end

    it "raises on invalid JSON response" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 200, body: "{not a valid json")

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      expect { sel.init }.to raise_error(Pinot::ConfigurationError, /decoding controller API response/)
    end
  end

  describe "#build_controller_url" do
    let(:sel) { Pinot::ControllerBasedBrokerSelector.new(Pinot::ControllerConfig.new) }

    it "adds http:// when no scheme" do
      expect(sel.build_controller_url("localhost:9000"))
        .to eq "http://localhost:9000/v2/brokers/tables?state=ONLINE"
    end

    it "keeps https:// scheme" do
      expect(sel.build_controller_url("https://host:1234"))
        .to eq "https://host:1234/v2/brokers/tables?state=ONLINE"
    end

    it "keeps http:// scheme" do
      expect(sel.build_controller_url("http://host:1234"))
        .to eq "http://host:1234/v2/brokers/tables?state=ONLINE"
    end

    it "raises for unsupported scheme" do
      expect { sel.build_controller_url("smb://nope:1234") }
        .to raise_error(Pinot::ConfigurationError, /unsupported controller URL scheme: smb/)
    end
  end

  describe "background refresh" do
    it "updates broker data after polling interval" do
      first = true
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return do
          if first
            first = false
            { status: 200, body: '{"baseballStats":[{"port":8000,"host":"host1","instanceName":"Broker_host1_8000"}]}' }
          else
            { status: 200, body: '{"baseballStats":[{"port":8000,"host":"host2","instanceName":"Broker_host2_8000"}]}' }
          end
        end

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000", update_freq_ms: 500)
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      sel.init

      expect(sel.select_broker("baseballStats")).to eq "host1:8000"

      sleep 0.7

      expect(sel.select_broker("baseballStats")).to eq "host2:8000"
    end
  end

  describe "#build_controller_url additional scheme cases" do
    let(:sel) { Pinot::ControllerBasedBrokerSelector.new(Pinot::ControllerConfig.new) }

    it "keeps https:// scheme from an address that starts with https://" do
      url = sel.build_controller_url("https://secure-controller:9443")
      expect(url).to start_with("https://")
      expect(url).to include("/v2/brokers/tables?state=ONLINE")
    end

    it "raises ConfigurationError for unsupported scheme ftp://" do
      expect { sel.build_controller_url("ftp://badhost:21") }
        .to raise_error(Pinot::ConfigurationError, /unsupported controller URL scheme: ftp/)
    end
  end

  describe "#fetch_and_update" do
    it "raises TransportError on HTTP 500" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 500, body: "Internal Server Error")

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      expect { sel.init }.to raise_error(Pinot::TransportError, /500/)
    end

    it "updates broker data with valid JSON response" do
      valid_body = '{"myTable":[{"port":7777,"host":"newhost","instanceName":"Broker_newhost_7777"}]}'
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 200, body: valid_body, headers: { "Content-Type" => "application/json" })

      config = Pinot::ControllerConfig.new(controller_address: "localhost:9000")
      sel = Pinot::ControllerBasedBrokerSelector.new(config)
      sel.init

      expect(sel.select_broker("myTable")).to eq "newhost:7777"
    end
  end
end
