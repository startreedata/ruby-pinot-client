RSpec.describe "Pinot factory methods" do
  let(:sql_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[97889]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}'
  end

  describe "Pinot.from_broker_list" do
    it "returns a Connection with SimpleBrokerSelector" do
      conn = Pinot.from_broker_list(["localhost:8000"])
      expect(conn).to be_a(Pinot::Connection)
      selector = conn.instance_variable_get(:@broker_selector)
      expect(selector).to be_a(Pinot::SimpleBrokerSelector)
    end

    it "executes SQL successfully" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = Pinot.from_broker_list(["localhost:8000"])
      resp = conn.execute_sql("", "select count(*) from t")
      expect(resp).to be_a(Pinot::BrokerResponse)
    end
  end

  describe "Pinot.from_controller" do
    it "returns a Connection with ControllerBasedBrokerSelector" do
      stub_request(:get, "http://localhost:9000/v2/brokers/tables?state=ONLINE")
        .to_return(status: 200, body: '{"baseballStats":[{"port":8000,"host":"h1","instanceName":"Broker_h1_8000"}]}')

      conn = Pinot.from_controller("localhost:9000")
      expect(conn).to be_a(Pinot::Connection)
      selector = conn.instance_variable_get(:@broker_selector)
      expect(selector).to be_a(Pinot::ControllerBasedBrokerSelector)
    end
  end

  describe "Pinot.from_config" do
    it "raises when no broker source specified" do
      config = Pinot::ClientConfig.new
      expect { Pinot.from_config(config) }.to raise_error(Pinot::ConfigurationError, /must specify broker_list or controller_config/)
    end

    it "passes extra_http_header to transport" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "X-Auth" => "token123" })
        .to_return(status: 200, body: sql_response)

      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8000"],
        extra_http_header: { "X-Auth" => "token123" }
      )
      conn = Pinot.from_config(config)
      conn.execute_sql("", "select count(*) from t")
    end

    it "sets use_multistage_engine on connection" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("useMultistageEngine=true") }
        .to_return(status: 200, body: sql_response)

      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8000"],
        use_multistage_engine: true
      )
      conn = Pinot.from_config(config)
      conn.execute_sql("", "select count(*) from t")
    end

    it "passes http_timeout to HttpClient as timeout:" do
      fake_client = instance_double(Pinot::HttpClient)
      expect(Pinot::HttpClient).to receive(:new).with(timeout: 15, tls_config: nil).and_return(fake_client)

      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8000"],
        http_timeout: 15
      )
      Pinot.from_config(config)
    end

    it "uses ZookeeperBrokerSelector when zookeeper_config is set" do
      fake_zk = double("FakeZK")
      allow(fake_zk).to receive(:get).and_return([
        JSON.dump("mapFields" => { "myTable_OFFLINE" => { "Broker_host1_8000" => "ONLINE" } }),
        nil
      ])
      allow(fake_zk).to receive(:register)
      allow(fake_zk).to receive(:exists?).and_return(true)

      zk_config = Pinot::ZookeeperConfig.new(zk_path: "localhost:2181")
      config = Pinot::ClientConfig.new(zookeeper_config: zk_config)

      allow(Pinot::ZookeeperBrokerSelector).to receive(:new).and_wrap_original do |orig, **kwargs|
        orig.call(zk_client: fake_zk, **kwargs)
      end

      conn = Pinot.from_config(config)
      expect(conn).to be_a(Pinot::Connection)
      selector = conn.instance_variable_get(:@broker_selector)
      expect(selector).to be_a(Pinot::ZookeeperBrokerSelector)
    end

    it "uses GrpcTransport and SimpleBrokerSelector when grpc_config is set", skip: !defined?(Pinot::GrpcTransport) do
      grpc_cfg = Pinot::GrpcConfig.new(broker_list: ["grpc-host:8090"])
      config = Pinot::ClientConfig.new(grpc_config: grpc_cfg)

      conn = Pinot.from_config(config)

      expect(conn).to be_a(Pinot::Connection)

      transport = conn.instance_variable_get(:@transport)
      expect(transport).to be_a(Pinot::GrpcTransport)

      selector = conn.instance_variable_get(:@broker_selector)
      expect(selector).to be_a(Pinot::SimpleBrokerSelector)
      expect(selector.select_broker("")).to eq("grpc-host:8090")
    end
  end
end
