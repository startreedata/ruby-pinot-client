RSpec.describe Pinot::Connection, "#execute_many" do
  let(:ok_body) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[1]]},' \
    '"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":2}'
  end

  def build_connection(broker: "localhost:8000")
    selector  = Pinot::SimpleBrokerSelector.new([broker])
    transport = Pinot::JsonHttpTransport.new(http_client: Pinot::HttpClient.new, extra_headers: {})
    conn      = Pinot::Connection.new(transport: transport, broker_selector: selector)
    selector.init
    conn
  end

  describe "empty input" do
    it "returns an empty array immediately" do
      expect(build_connection.execute_many([])).to eq []
    end
  end

  describe "result ordering" do
    it "returns results in the same order as the input" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: ok_body)

      queries = [
        { table: "t1", query: "select 1 from t1" },
        { table: "t2", query: "select 2 from t2" },
        { table: "t3", query: "select 3 from t3" }
      ]
      results = build_connection.execute_many(queries)

      expect(results.size).to eq 3
      expect(results[0].table).to eq "t1"
      expect(results[1].table).to eq "t2"
      expect(results[2].table).to eq "t3"
    end
  end

  describe "successful queries" do
    it "returns QueryResult with response set and error nil" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: ok_body)

      results = build_connection.execute_many([{ table: "t", query: "select 1" }])

      expect(results.first).to be_a(Pinot::QueryResult)
      expect(results.first).to be_success
      expect(results.first).not_to be_error
      expect(results.first.response).to be_a(Pinot::BrokerResponse)
      expect(results.first.error).to be_nil
    end
  end

  describe "error isolation" do
    it "captures the error in QueryResult without raising" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 503, body: "")

      results = build_connection.execute_many([{ table: "t", query: "select 1" }])

      expect(results.first).to be_error
      expect(results.first.error).to be_a(Pinot::BrokerUnavailableError)
      expect(results.first.response).to be_nil
    end

    it "isolates errors per query — successes and failures coexist" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("good") }
        .to_return(status: 200, body: ok_body)

      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("bad") }
        .to_return(status: 503, body: "")

      queries = [
        { table: "t", query: "select good" },
        { table: "t", query: "select bad" },
        { table: "t", query: "select good" }
      ]
      results = build_connection.execute_many(queries)

      expect(results[0]).to be_success
      expect(results[1]).to be_error
      expect(results[2]).to be_success
    end
  end

  describe "per-query timeout" do
    it "passes query_timeout_ms from each item to execute_sql" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=1234") }
        .to_return(status: 200, body: ok_body)

      queries = [{ table: "t", query: "select 1", query_timeout_ms: 1234 }]
      results = build_connection.execute_many(queries)

      expect(results.first).to be_success
    end
  end

  describe "max_concurrency" do
    it "still returns all results when max_concurrency is set" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: ok_body)

      queries = Array.new(5) { { table: "t", query: "select 1" } }
      results = build_connection.execute_many(queries, max_concurrency: 2)

      expect(results.size).to eq 5
      expect(results).to all(be_success)
    end

    it "limits in-flight threads to max_concurrency" do
      concurrent_peak = Concurrent::AtomicFixnum.new(0) rescue nil
      active          = Mutex.new
      active_count    = 0
      peak            = 0

      stub_request(:post, "http://localhost:8000/query/sql").to_return do
        active.synchronize do
          active_count += 1
          peak = active_count if active_count > peak
        end
        sleep 0.01
        active.synchronize { active_count -= 1 }
        { status: 200, body: ok_body }
      end

      queries = Array.new(6) { { table: "t", query: "select 1" } }
      build_connection.execute_many(queries, max_concurrency: 2)

      expect(peak).to be <= 2
    end

    it "raises ArgumentError when max_concurrency is less than 1" do
      expect { build_connection.execute_many([{ table: "t", query: "q" }], max_concurrency: 0) }
        .to raise_error(ArgumentError)
    end
  end

  describe "QueryResult struct" do
    it "exposes table, query, response, and error" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: ok_body)

      result = build_connection.execute_many([{ table: "myTable", query: "select 1" }]).first

      expect(result.table).to eq "myTable"
      expect(result.query).to eq "select 1"
      expect(result.response).to be_a(Pinot::BrokerResponse)
      expect(result.error).to be_nil
    end

    it "supports string-keyed query hashes" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: ok_body)

      results = build_connection.execute_many([{ "table" => "t", "query" => "select 1" }])
      expect(results.first).to be_success
    end
  end

  describe "concurrency — all queries run in parallel" do
    it "executes N queries faster than N × single-query time" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return do
          sleep 0.05
          { status: 200, body: ok_body }
        end

      queries = Array.new(4) { { table: "t", query: "select 1" } }

      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      results = build_connection.execute_many(queries)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start

      expect(results).to all(be_success)
      # 4 × 50ms serial = 200ms; parallel should finish well under 150ms
      expect(elapsed).to be < 0.15
    end
  end
end

RSpec.describe Pinot::QueryResult do
  describe "#success? / #error?" do
    it "is success when error is nil" do
      qr = described_class.new(table: "t", query: "q", response: double, error: nil)
      expect(qr).to be_success
      expect(qr).not_to be_error
    end

    it "is error when error is set" do
      qr = described_class.new(table: "t", query: "q", response: nil, error: RuntimeError.new("boom"))
      expect(qr).to be_error
      expect(qr).not_to be_success
    end
  end
end
