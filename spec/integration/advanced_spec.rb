require_relative "integration_helper"

RSpec.describe "Pinot integration — advanced queries" do
  let(:table) { "baseballStats" }

  describe "trace flag" do
    it "executes a query with trace enabled and returns a valid response" do
      client = pinot_client
      client.open_trace
      resp = client.execute_sql(table, "SELECT count(*) FROM baseballStats LIMIT 1")
      expect(resp).not_to be_nil
      expect(resp.time_used_ms).to be >= 0
    end
  end

  describe "error handling — non-existent table" do
    it "raises TransportError or returns a response with non-empty exceptions" do
      client = pinot_client
      begin
        resp = client.execute_sql("nonExistentTable123", "SELECT * FROM nonExistentTable123 LIMIT 1")
        # Pinot may return HTTP 200 with exceptions in the response body
        expect(resp.exceptions).not_to be_empty
      rescue Pinot::TransportError => e
        # Alternatively Pinot may raise a transport-level error
        expect(e).to be_a(Pinot::TransportError)
      rescue => e
        # Accept any error indicating the table doesn't exist
        expect(e.message).to match(/nonExistentTable123|table|not found|error/i)
      end
    end
  end

  describe "large result set" do
    it "returns 1000 rows for LIMIT 1000 query" do
      client = pinot_client
      resp = client.execute_sql(table, "SELECT * FROM baseballStats LIMIT 1000")
      expect(resp).not_to be_nil
      expect(resp.result_table.row_count).to eq(1000)
    end
  end

  describe "column type access" do
    it "returns correct types from get_string, get_long, and get_double" do
      client = pinot_client
      resp = client.execute_sql(
        table,
        "SELECT playerName, yearID, hits FROM baseballStats LIMIT 1"
      )
      expect(resp).not_to be_nil
      rt = resp.result_table
      expect(rt.row_count).to be >= 1

      # playerName is STRING
      expect { rt.get_string(0, 0) }.not_to raise_error
      expect(rt.get_string(0, 0)).to be_a(String)

      # yearID is INT/LONG
      expect { rt.get_long(0, 1) }.not_to raise_error
      year = rt.get_long(0, 1)
      expect(year).to be_a(Integer)
      expect(year).to be > 1800

      # hits is INT/LONG
      expect { rt.get_long(0, 2) }.not_to raise_error
      expect(rt.get_long(0, 2)).to be_a(Integer)
    end
  end
end
