require_relative "integration_helper"

RSpec.describe "Pinot integration — batch queries" do
  let(:table) { "baseballStats" }
  let(:count_query) { "select count(*) as cnt from baseballStats limit 1" }

  describe "from_broker_list" do
    it "executes count(*) and returns 97889" do
      client = pinot_client
      resp = client.execute_sql(table, count_query)
      expect(resp).not_to be_nil
      expect(resp.result_table.get_long(0, 0)).to eq 97_889
    end

    it "executes count(*) with multistage engine and returns 97889" do
      client = pinot_client(use_multistage: true)
      resp = client.execute_sql(table, count_query)
      expect(resp.result_table.get_long(0, 0)).to eq 97_889
    end
  end

  describe "from_config with timeout" do
    it "executes count(*) with ClientConfig timeout" do
      config = Pinot::ClientConfig.new(
        broker_list: ["#{BROKER_HOST}:#{BROKER_PORT}"],
        http_timeout: 10,
        extra_http_header: {}
      )
      client = Pinot.from_config(config)
      resp = client.execute_sql(table, count_query)
      expect(resp.result_table.get_long(0, 0)).to eq 97_889
    end
  end

  describe "repeated queries (200 iterations)" do
    it "succeeds consistently" do
      client = pinot_client
      200.times do
        resp = client.execute_sql(table, count_query)
        expect(resp.result_table.get_long(0, 0)).to eq 97_889
      end
    end
  end

  describe "execute_sql_with_params" do
    it "substitutes integer param" do
      client = pinot_client
      resp = client.execute_sql_with_params(
        table,
        "select count(*) as cnt from baseballStats where yearID = ? limit 1",
        [2000]
      )
      expect(resp).not_to be_nil
      expect(resp.result_table.get_long(0, 0)).to be > 0
    end

    it "substitutes string param" do
      client = pinot_client
      resp = client.execute_sql_with_params(
        table,
        "select count(*) as cnt from baseballStats where teamID = ? limit 1",
        ["SFN"]
      )
      expect(resp.result_table.get_long(0, 0)).to be > 0
    end
  end
end
