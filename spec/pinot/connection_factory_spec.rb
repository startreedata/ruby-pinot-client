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
      expect { Pinot.from_config(config) }.to raise_error(ArgumentError, /must specify broker_list or controller_config/)
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
      expect(Pinot::HttpClient).to receive(:new).with(timeout: 15).and_return(fake_client)

      config = Pinot::ClientConfig.new(
        broker_list: ["localhost:8000"],
        http_timeout: 15
      )
      Pinot.from_config(config)
    end
  end
end
