RSpec.describe Pinot::SchemaClient do
  let(:base_url)  { "http://controller:9000" }
  let(:client)    { described_class.new(base_url) }

  let(:tables_response) do
    { "tables" => %w[baseballStats orders products] }.to_json
  end

  let(:schema_response) do
    {
      "schemaName" => "baseballStats",
      "dimensionFieldSpecs" => [
        { "name" => "playerID",   "dataType" => "STRING" },
        { "name" => "teamID",     "dataType" => "STRING" }
      ],
      "metricFieldSpecs" => [
        { "name" => "homeRuns", "dataType" => "INT" }
      ],
      "dateTimeFieldSpecs" => [
        { "name" => "yearID", "dataType" => "INT" }
      ]
    }.to_json
  end

  let(:table_config_response) do
    { "OFFLINE" => { "tableName" => "baseballStats_OFFLINE" } }.to_json
  end

  # ── list_tables ─────────────────────────────────────────────────────────────

  describe "#list_tables" do
    it "returns an array of table name strings" do
      stub_request(:get, "#{base_url}/tables")
        .to_return(status: 200, body: tables_response)

      expect(client.list_tables).to eq(%w[baseballStats orders products])
    end

    it "returns an empty array when tables key is absent" do
      stub_request(:get, "#{base_url}/tables")
        .to_return(status: 200, body: "{}".to_json)

      expect(client.list_tables).to eq([])
    end

    it "raises TransportError on non-200" do
      stub_request(:get, "#{base_url}/tables")
        .to_return(status: 500, body: "error")

      expect { client.list_tables }.to raise_error(Pinot::TransportError, /500/)
    end
  end

  # ── get_schema ──────────────────────────────────────────────────────────────

  describe "#get_schema" do
    it "returns the raw schema Hash" do
      stub_request(:get, "#{base_url}/schemas/baseballStats")
        .to_return(status: 200, body: schema_response)

      result = client.get_schema("baseballStats")
      expect(result["schemaName"]).to eq("baseballStats")
      expect(result["dimensionFieldSpecs"].size).to eq(2)
    end

    it "raises TableNotFoundError on 404" do
      stub_request(:get, "#{base_url}/schemas/missing")
        .to_return(status: 404, body: "")

      expect { client.get_schema("missing") }
        .to raise_error(Pinot::TableNotFoundError, /missing/)
    end

    it "raises TransportError on 500" do
      stub_request(:get, "#{base_url}/schemas/baseballStats")
        .to_return(status: 500, body: "")

      expect { client.get_schema("baseballStats") }
        .to raise_error(Pinot::TransportError, /500/)
    end

    it "raises TransportError on invalid JSON" do
      stub_request(:get, "#{base_url}/schemas/baseballStats")
        .to_return(status: 200, body: "not-json{{{")

      expect { client.get_schema("baseballStats") }
        .to raise_error(Pinot::TransportError, /invalid JSON/)
    end
  end

  # ── get_table_config ────────────────────────────────────────────────────────

  describe "#get_table_config" do
    it "returns the raw table config Hash" do
      stub_request(:get, "#{base_url}/tables/baseballStats")
        .to_return(status: 200, body: table_config_response)

      result = client.get_table_config("baseballStats")
      expect(result["OFFLINE"]["tableName"]).to eq("baseballStats_OFFLINE")
    end

    it "raises TableNotFoundError on 404" do
      stub_request(:get, "#{base_url}/tables/missing")
        .to_return(status: 404, body: "")

      expect { client.get_table_config("missing") }
        .to raise_error(Pinot::TableNotFoundError)
    end
  end

  # ── table_exists? ────────────────────────────────────────────────────────────

  describe "#table_exists?" do
    it "returns true when the table exists" do
      stub_request(:get, "#{base_url}/tables/orders")
        .to_return(status: 200, body: "{}")

      expect(client.table_exists?("orders")).to be true
    end

    it "returns false when the table does not exist (404)" do
      stub_request(:get, "#{base_url}/tables/ghost")
        .to_return(status: 404, body: "")

      expect(client.table_exists?("ghost")).to be false
    end
  end

  # ── column_types ─────────────────────────────────────────────────────────────

  describe "#column_types" do
    before do
      stub_request(:get, "#{base_url}/schemas/baseballStats")
        .to_return(status: 200, body: schema_response)
    end

    it "returns a flat name => dataType hash" do
      result = client.column_types("baseballStats")
      expect(result).to eq(
        "playerID" => "STRING",
        "teamID" => "STRING",
        "homeRuns" => "INT",
        "yearID" => "INT"
      )
    end
  end

  # ── address normalisation ────────────────────────────────────────────────────

  describe "controller address normalisation" do
    it "prepends http:// when scheme is missing" do
      stub_request(:get, "http://controller:9000/tables")
        .to_return(status: 200, body: tables_response)

      described_class.new("controller:9000").list_tables
      expect(a_request(:get, "http://controller:9000/tables")).to have_been_made
    end

    it "preserves an explicit http:// scheme" do
      stub_request(:get, "http://controller:9000/tables")
        .to_return(status: 200, body: tables_response)

      described_class.new("http://controller:9000").list_tables
      expect(a_request(:get, "http://controller:9000/tables")).to have_been_made
    end

    it "preserves an https:// scheme" do
      stub_request(:get, "https://secure:9000/tables")
        .to_return(status: 200, body: tables_response)

      described_class.new("https://secure:9000").list_tables
      expect(a_request(:get, "https://secure:9000/tables")).to have_been_made
    end

    it "strips a trailing slash from the address" do
      stub_request(:get, "http://controller:9000/tables")
        .to_return(status: 200, body: tables_response)

      described_class.new("http://controller:9000/").list_tables
      expect(a_request(:get, "http://controller:9000/tables")).to have_been_made
    end
  end

  # ── extra headers ────────────────────────────────────────────────────────────

  describe "extra headers" do
    it "forwards custom headers on every request" do
      stub = stub_request(:get, "#{base_url}/tables")
               .with(headers: { "Authorization" => "Bearer token123" })
               .to_return(status: 200, body: tables_response)

      described_class.new(base_url, headers: { "Authorization" => "Bearer token123" }).list_tables
      expect(stub).to have_been_requested
    end
  end
end
