require "bigdecimal"

RSpec.describe Pinot::Connection do
  let(:broker_url) { "http://localhost:8000" }
  let(:sql_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[97889]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}'
  end

  def build_connection(broker: "localhost:8000", use_multistage: false, query_timeout_ms: nil)
    selector = Pinot::SimpleBrokerSelector.new([broker])
    transport = Pinot::JsonHttpTransport.new(
      http_client: Pinot::HttpClient.new,
      extra_headers: {}
    )
    conn = Pinot::Connection.new(
      transport: transport,
      broker_selector: selector,
      use_multistage_engine: use_multistage,
      query_timeout_ms: query_timeout_ms
    )
    selector.init
    conn
  end

  describe "#execute_sql instrumentation" do
    let(:conn) { build_connection }

    before { Pinot::Instrumentation.on_query = nil }
    after  { Pinot::Instrumentation.on_query = nil }

    it "triggers the instrumentation callback" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      received = nil
      Pinot::Instrumentation.on_query = proc { |event| received = event }

      conn = build_connection
      conn.execute_sql("myTable", "select count(*) from myTable")

      expect(received).not_to be_nil
      expect(received[:table]).to eq("myTable")
      expect(received[:query]).to eq("select count(*) from myTable")
      expect(received[:success]).to be true
      expect(received[:error]).to be_nil
      expect(received[:duration_ms]).to be_a(Float)
    end

    it "triggers the instrumentation callback with success: false on error" do
      received = nil
      Pinot::Instrumentation.on_query = proc { |event| received = event }

      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 500, body: "")

      expect { conn.execute_sql("myTable", "select 1") }.to raise_error(Pinot::TransportError)

      expect(received).not_to be_nil
      expect(received[:table]).to eq("myTable")
      expect(received[:success]).to be false
      expect(received[:error]).not_to be_nil
      expect(received[:duration_ms]).to be_a(Float)
    end

    it "passes per-call query_timeout_ms to the request" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=9999") }
        .to_return(status: 200, body: '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[1]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":1}')

      conn.execute_sql("myTable", "select 1", query_timeout_ms: 9999)
    end
  end

  describe "#execute_sql" do
    it "returns BrokerResponse on success" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      resp = conn.execute_sql("", "select count(*) from t")
      expect(resp).to be_a(Pinot::BrokerResponse)
      expect(resp.result_table.get_long(0, 0)).to eq 97_889
    end

    it "raises when broker selector raises" do
      selector = double("selector")
      allow(selector).to receive(:select_broker).and_raise("error selecting broker")
      transport = double("transport")
      conn = described_class.new(transport: transport, broker_selector: selector)
      expect { conn.execute_sql("", "q") }.to raise_error(/error selecting broker/)
    end

    it "raises on HTTP error" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 400, body: "")

      conn = build_connection
      expect { conn.execute_sql("", "select count(*) from t") }.to raise_error(/400/)
    end

    it "raises on non-JSON response with parse error" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: "ProcessingException", headers: { "Content-Type" => "application/json" })

      conn = build_connection
      expect { conn.execute_sql("", "select count(*) from t") }.to raise_error(/unexpected (token|character)/i)
    end

    it "propagates TransportError without wrapping" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 503, body: "")
      conn = build_connection
      expect { conn.execute_sql("t", "select 1") }.to raise_error(Pinot::TransportError)
    end
  end

  describe "trace control" do
    it "includes trace=true when open_trace called" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["trace"] == "true" }
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      conn.open_trace
      conn.execute_sql("", "select count(*) from t")
    end

    it "does not include trace after close_trace" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| !JSON.parse(r.body).key?("trace") }
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      conn.open_trace
      conn.close_trace
      conn.execute_sql("", "select count(*) from t")
    end
  end

  describe "query_timeout_ms" do
    it "passes query_timeout_ms from config to the request" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=3000") }
        .to_return(status: 200, body: sql_response)

      conn = build_connection(query_timeout_ms: 3000)
      conn.execute_sql("", "select count(*) from t")
    end

    it "execute_sql_with_timeout overrides the timeout for a single query" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=7500") }
        .to_return(status: 200, body: sql_response)

      conn = build_connection(query_timeout_ms: 3000)
      resp = conn.execute_sql_with_timeout("", "select count(*) from t", 7500)
      expect(resp).to be_a(Pinot::BrokerResponse)
    end

    it "execute_sql_with_timeout does not permanently change the connection timeout" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = build_connection(query_timeout_ms: 3000)
      conn.execute_sql_with_timeout("", "select count(*) from t", 7500)
      expect(conn.query_timeout_ms).to eq 3000
    end
  end

  describe "#use_multistage_engine=" do
    it "sets multistage engine flag" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("useMultistageEngine=true") }
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      conn.use_multistage_engine = true
      conn.execute_sql("", "select count(*) from t")
    end
  end

  describe "#execute_sql_with_params" do
    it "substitutes single integer param" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"] == "SELECT * FROM table WHERE id = 42" }
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      resp = conn.execute_sql_with_params("", "SELECT * FROM table WHERE id = ?", [42])
      expect(resp).to be_a(Pinot::BrokerResponse)
    end

    it "substitutes string and integer params" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"] == "SELECT * FROM table WHERE id = 42 AND name = 'John'" }
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      conn.execute_sql_with_params("", "SELECT * FROM table WHERE id = ? AND name = ?", [42, "John"])
    end

    it "raises with param count mismatch" do
      conn = build_connection
      expect do
        conn.execute_sql_with_params("", "SELECT * FROM table WHERE id = ? AND name = ?", [42])
      end.to raise_error("failed to format query: number of placeholders in queryPattern (2) does not match number of params (1)")
    end

    it "raises for unsupported type" do
      conn = build_connection
      expect do
        conn.execute_sql_with_params("", "SELECT * FROM table WHERE id = ?", [Object.new])
      end.to raise_error(/failed to format query: failed to format parameter: unsupported type: Object/)
    end

    it "escapes single quotes in string params" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"] == "SELECT * FROM table WHERE name = 'John''s'" }
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      conn.execute_sql_with_params("", "SELECT * FROM table WHERE name = ?", ["John's"])
    end

    it "raises when broker selector raises" do
      selector = double("selector")
      allow(selector).to receive(:select_broker).and_raise("error selecting broker")
      transport = double("transport")
      conn = described_class.new(transport: transport, broker_selector: selector)
      expect do
        conn.execute_sql_with_params("baseballStats2", "SELECT * FROM t WHERE id = ?", [42])
      end.to raise_error(/error selecting broker/)
    end
  end

  describe "#format_query" do
    let(:conn) { described_class.new(transport: double, broker_selector: double) }

    it "no params" do
      expect(conn.format_query("SELECT * FROM table", [])).to eq "SELECT * FROM table"
    end

    it "nil params treated as empty" do
      expect(conn.format_query("SELECT * FROM table", nil)).to eq "SELECT * FROM table"
    end

    it "single integer param" do
      expect(conn.format_query("SELECT * FROM table WHERE id = ?", [42])).to eq "SELECT * FROM table WHERE id = 42"
    end

    it "string and integer params" do
      result = conn.format_query("SELECT * FROM table WHERE id = ? AND name = ?", [42, "John"])
      expect(result).to eq "SELECT * FROM table WHERE id = 42 AND name = 'John'"
    end

    it "raises on param count mismatch" do
      expect do
        conn.format_query("SELECT * FROM table WHERE id = ? AND name = ?", [42])
      end.to raise_error("failed to format query: number of placeholders in queryPattern (2) does not match number of params (1)")
    end

    it "escapes single quotes" do
      result = conn.format_query("SELECT * FROM table WHERE name = ?", ["John's"])
      expect(result).to eq "SELECT * FROM table WHERE name = 'John''s'"
    end
  end

  describe "#format_arg" do
    let(:conn) { described_class.new(transport: double, broker_selector: double) }

    it "string value" do
      expect(conn.format_arg("hello")).to eq "'hello'"
    end

    it "Time value" do
      t = Time.new(2022, 1, 1, 12, 0, 0, 0)
      expect(conn.format_arg(t)).to eq "'2022-01-01 12:00:00.000'"
    end

    it "integer value" do
      expect(conn.format_arg(42)).to eq "42"
    end

    it "BigDecimal value (like big.Int)" do
      expect(conn.format_arg(BigDecimal("1234567890"))).to eq "'1234567890'"
    end

    it "float32 value" do
      expect(conn.format_arg(3.14)).to eq "3.14"
    end

    it "float64 value" do
      expect(conn.format_arg(3.14159)).to eq "3.14159"
    end

    it "bool true value" do
      expect(conn.format_arg(true)).to eq "true"
    end

    it "bool false value" do
      expect(conn.format_arg(false)).to eq "false"
    end

    it "unsupported type raises error" do
      expect { conn.format_arg({}) }.to raise_error("unsupported type: Hash")
    end

    it "big float (BigDecimal with decimals)" do
      bd = BigDecimal("3.141592653589793")
      expect(conn.format_arg(bd)).to eq "'3.141592653589793'"
    end

    it "negative BigDecimal retains negative sign" do
      expect(conn.format_arg(BigDecimal("-123.45"))).to eq "'-123.45'"
    end

    it "BigDecimal with decimal places retained" do
      expect(conn.format_arg(BigDecimal("1.5"))).to eq "'1.5'"
    end

    it "Time with zero milliseconds has no rounding artifacts" do
      t = Time.utc(2023, 6, 15, 10, 30, 0)
      result = conn.format_arg(t)
      expect(result).to eq "'2023-06-15 10:30:00.000'"
    end

    it "String with multiple single quotes escapes all of them" do
      expect(conn.format_arg("it's a test's value")).to eq "'it''s a test''s value'"
    end
  end

  describe "#execute_sql_with_params forwards query_timeout_ms" do
    it "sends timeoutMs in the request body when query_timeout_ms is provided" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=7777") }
        .to_return(status: 200, body: sql_response)

      conn = build_connection(query_timeout_ms: 7777)
      resp = conn.execute_sql_with_params("", "SELECT ? LIMIT 1", [1])
      expect(resp).to be_a(Pinot::BrokerResponse)
    end
  end

  describe "per-request headers" do
    it "forwards headers: kwarg from execute_sql to transport" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "X-Auth-Token" => "secret" })
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      resp = conn.execute_sql("", "select 1", headers: { "X-Auth-Token" => "secret" })
      expect(resp).to be_a(Pinot::BrokerResponse)
    end

    it "forwards headers: kwarg from execute_sql_with_params to transport" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "X-Trace-Id" => "abc123" })
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      resp = conn.execute_sql_with_params("", "select ?", [1], headers: { "X-Trace-Id" => "abc123" })
      expect(resp).to be_a(Pinot::BrokerResponse)
    end

    it "works without headers: kwarg (backward compatible)" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      expect { conn.execute_sql("", "select 1") }.not_to raise_error
    end

    it "per-request headers are isolated between calls" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      conn = build_connection
      conn.execute_sql("", "select 1", headers: { "X-Call" => "first" })

      # Second call without headers should not include the first call's header
      stub = stub_request(:post, "http://localhost:8000/query/sql")
               .with { |r| !r.headers.key?("X-Call") }
               .to_return(status: 200, body: sql_response)

      conn.execute_sql("", "select 1")
      expect(stub).to have_been_requested
    end
  end
end
