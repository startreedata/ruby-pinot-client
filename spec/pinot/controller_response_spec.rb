RSpec.describe Pinot::ControllerResponse do
  describe "single table, single broker" do
    let(:raw) do
      {
        "baseballStats" => [
          { "host" => "h1", "port" => 8000, "instanceName" => "Broker_h1_8000" }
        ]
      }
    end

    subject(:cr) { Pinot::ControllerResponse.new(raw) }

    it "extract_broker_list" do
      expect(cr.extract_broker_list).to eq ["h1:8000"]
    end

    it "extract_table_to_broker_map" do
      expect(cr.extract_table_to_broker_map).to eq({ "baseballStats" => ["h1:8000"] })
    end
  end

  describe "multiple tables, shared brokers" do
    let(:raw) do
      {
        "table1" => [
          { "host" => "h1", "port" => 8000, "instanceName" => "Broker_h1_8000" },
          { "host" => "h2", "port" => 8000, "instanceName" => "Broker_h2_8000" }
        ],
        "table2" => [
          { "host" => "h1", "port" => 8000, "instanceName" => "Broker_h1_8000" }
        ]
      }
    end

    subject(:cr) { Pinot::ControllerResponse.new(raw) }

    it "deduplicates broker list" do
      list = cr.extract_broker_list
      expect(list.uniq).to eq list
      expect(list).to include("h1:8000", "h2:8000")
    end

    it "extract_table_to_broker_map has all tables" do
      map = cr.extract_table_to_broker_map
      expect(map["table1"]).to include("h1:8000", "h2:8000")
      expect(map["table2"]).to eq ["h1:8000"]
    end
  end
end
