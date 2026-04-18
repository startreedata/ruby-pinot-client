require "time"

RSpec.describe Pinot::PreparedStatementImpl do
  let(:sql_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG","STRING"],"columnNames":["id","name"]},"rows":[[123,"testName"]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}'
  end

  let(:multi_row_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG","STRING","LONG"],"columnNames":["id","name","age"]},"rows":[[123,"testName",25]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}'
  end

  def build_connection
    stub_request(:get, "http://localhost:8000/v2/brokers/tables?state=ONLINE")
      .to_return(status: 200, body: "{}", headers: {})
    Pinot.from_broker_list(["localhost:8000"])
  end

  describe "Connection#prepare validation" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "succeeds with single parameter" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      expect(stmt).not_to be_nil
      expect(stmt.get_query).to eq "SELECT * FROM testTable WHERE id = ?"
      expect(stmt.get_parameter_count).to eq 1
    end

    it "succeeds with multiple parameters" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?")
      expect(stmt.get_parameter_count).to eq 3
    end

    it "raises for empty table name" do
      expect { conn.prepare("", "SELECT * FROM t WHERE id = ?") }
        .to raise_error(ArgumentError, /table name cannot be empty/)
    end

    it "raises for empty query" do
      expect { conn.prepare("testTable", "") }
        .to raise_error(ArgumentError, /query template cannot be empty/)
    end

    it "raises for query without placeholders" do
      expect { conn.prepare("testTable", "SELECT * FROM testTable") }
        .to raise_error(ArgumentError, /query template must contain at least one parameter placeholder/)
    end
  end

  describe "#set / typed setters" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }
    let(:stmt) { conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?") }

    it "set_int succeeds for valid index" do
      expect { stmt.set_int(1, 123) }.not_to raise_error
    end

    it "set_string succeeds for valid index" do
      expect { stmt.set_string(2, "testName") }.not_to raise_error
    end

    it "raises for index 0 (too low)" do
      expect { stmt.set_int(0, 123) }
        .to raise_error(/parameter index 0 is out of range/)
    end

    it "raises for index 3 (too high)" do
      expect { stmt.set_int(3, 123) }
        .to raise_error(/parameter index 3 is out of range/)
    end

    it "set_int64 succeeds" do
      expect { stmt.set_int64(1, 456) }.not_to raise_error
    end

    it "set_float64 succeeds" do
      expect { stmt.set_float64(1, 3.14) }.not_to raise_error
    end

    it "set_bool succeeds" do
      expect { stmt.set_bool(1, true) }.not_to raise_error
    end

    it "generic set succeeds" do
      expect { stmt.set(1, "genericValue") }.not_to raise_error
    end

    it "set with Time succeeds" do
      expect { stmt.set(1, Time.now) }.not_to raise_error
    end
  end

  describe "#execute with mock server" do
    it "raises when parameters not all set" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?")

      expect { stmt.execute }.to raise_error(/parameter at index 1 is not set/)
    end

    it "executes successfully when all params set" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?")
      stmt.set_int(1, 123)
      stmt.set_string(2, "testName")

      resp = stmt.execute
      expect(resp).not_to be_nil
      expect(resp.result_table).not_to be_nil
    end
  end

  describe "#execute_with_params" do
    it "executes with correct number of params" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: multi_row_response)

      conn = build_connection
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?")
      resp = stmt.execute_with_params(123, "testName", 18)
      expect(resp).not_to be_nil
    end

    it "raises with too few params" do
      conn = build_connection
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?")
      expect { stmt.execute_with_params(123) }
        .to raise_error(/expected 3 parameters, got 1/)
    end

    it "raises with too many params" do
      conn = build_connection
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?")
      expect { stmt.execute_with_params(123, "testName", 18, "extra") }
        .to raise_error(/expected 3 parameters, got 4/)
    end
  end

  describe "parameter formatting" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "formats 5 different param types correctly" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE col1 = ? AND col2 = ? AND col3 = ? AND col4 = ? AND col5 = ?")
      ps = stmt

      params = [
        "string_value",
        123,
        3.14,
        true,
        Time.utc(2023, 1, 1, 12, 0, 0)
      ]

      query = ps.build_query(params)
      expected = "SELECT * FROM testTable WHERE col1 = 'string_value' AND col2 = 123 AND col3 = 3.14 AND col4 = true AND col5 = '2023-01-01 12:00:00.000'"
      expect(query).to eq expected
    end
  end

  describe "#clear_parameters" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "clears all parameters" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ?")
      stmt.set_int(1, 123)
      stmt.set_string(2, "testName")
      stmt.clear_parameters

      expect { stmt.execute }.to raise_error(/parameter at index 1 is not set/)
    end
  end

  describe "#close" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "prevents set after close" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      stmt.close
      expect { stmt.set_int(1, 123) }.to raise_error(Pinot::PreparedStatementClosedError, /prepared statement is closed/)
    end

    it "prevents execute after close" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      stmt.close
      expect { stmt.execute }.to raise_error(Pinot::PreparedStatementClosedError, /prepared statement is closed/)
    end

    it "prevents execute_with_params after close" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      stmt.close
      expect { stmt.execute_with_params(123) }.to raise_error(Pinot::PreparedStatementClosedError, /prepared statement is closed/)
    end

    it "prevents clear_parameters after close" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      stmt.close
      expect { stmt.clear_parameters }.to raise_error(Pinot::PreparedStatementClosedError, /prepared statement is closed/)
    end
  end

  describe "#get_query and #get_parameter_count" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "returns query template" do
      template = "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?"
      stmt = conn.prepare("testTable", template)
      expect(stmt.get_query).to eq template
    end

    it "returns parameter count" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ? AND name = ? AND age > ?")
      expect(stmt.get_parameter_count).to eq 3
    end
  end

  describe "complex query formatting" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "formats complex baseball query" do
      stmt = conn.prepare("baseballStats",
        "SELECT playerName, sum(homeRuns) as totalHomeRuns " \
        "FROM baseballStats " \
        "WHERE homeRuns > ? AND teamID = ? AND yearID BETWEEN ? AND ? " \
        "GROUP BY playerID, playerName " \
        "ORDER BY totalHomeRuns DESC " \
        "LIMIT ?")

      expect(stmt.get_parameter_count).to eq 5

      params = [0, "OAK", 2000, 2010, 10]
      query = stmt.build_query(params)

      expected = "SELECT playerName, sum(homeRuns) as totalHomeRuns " \
        "FROM baseballStats " \
        "WHERE homeRuns > 0 AND teamID = 'OAK' AND yearID BETWEEN 2000 AND 2010 " \
        "GROUP BY playerID, playerName " \
        "ORDER BY totalHomeRuns DESC " \
        "LIMIT 10"
      expect(query).to eq expected
    end
  end

  describe "#build_query errors" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "raises with wrong param count" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      expect { stmt.build_query([]) }.to raise_error(/expected 1 parameters, got 0/)
    end

    it "raises with unsupported type" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      expect { stmt.build_query([{}]) }.to raise_error(/failed to format parameter/)
    end
  end

  describe "#execute_with_params format error" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "raises with 'failed to build query' message" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      expect { stmt.execute_with_params({}) }
        .to raise_error(/failed to build query/)
    end
  end

  describe "#execute format error" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "raises with 'failed to build query' when set param is unsupported type" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      stmt.set(1, {})
      expect { stmt.execute }.to raise_error(/failed to build query/)
    end
  end

  describe "concurrent usage" do
    let(:conn) { Pinot::Connection.new(transport: double, broker_selector: double) }

    it "handles concurrent parameter setting without crash" do
      stmt = conn.prepare("testTable", "SELECT * FROM testTable WHERE id = ?")
      done = Queue.new

      10.times do |i|
        Thread.new do
          stmt.set_int(1, i)
          done.push(true)
        end
      end

      10.times { done.pop }

      stmt.close
    end
  end
end
