RSpec.describe Pinot::Paginator do
  let(:schema) { { "columnDataTypes" => ["LONG", "STRING"], "columnNames" => ["id", "name"] } }

  def page_body(rows)
    JSON.generate(
      "resultTable" => { "dataSchema" => schema, "rows" => rows },
      "exceptions"  => [],
      "numServersQueried" => 1, "numServersResponded" => 1, "timeUsedMs" => 1
    )
  end

  def build_connection
    selector  = Pinot::SimpleBrokerSelector.new(["localhost:8000"])
    transport = Pinot::JsonHttpTransport.new(http_client: Pinot::HttpClient.new, extra_headers: {})
    conn      = Pinot::Connection.new(transport: transport, broker_selector: selector)
    selector.init
    conn
  end

  describe "argument validation" do
    let(:conn) { build_connection }

    it "raises when base query contains LIMIT" do
      expect { conn.paginate("t", "SELECT * FROM t LIMIT 10") }
        .to raise_error(ArgumentError, /LIMIT/)
    end

    it "raises when base query contains OFFSET" do
      expect { conn.paginate("t", "SELECT * FROM t OFFSET 0") }
        .to raise_error(ArgumentError, /OFFSET/)
    end

    it "is case-insensitive for LIMIT/OFFSET detection" do
      expect { conn.paginate("t", "SELECT * FROM t limit 10") }
        .to raise_error(ArgumentError)
    end

    it "raises when page_size is zero" do
      expect { conn.paginate("t", "SELECT * FROM t", page_size: 0) }
        .to raise_error(ArgumentError, /page_size/)
    end

    it "raises when page_size is negative" do
      expect { conn.paginate("t", "SELECT * FROM t", page_size: -1) }
        .to raise_error(ArgumentError, /page_size/)
    end
  end

  describe "#each_page" do
    it "yields nothing for an empty first page" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("LIMIT 2 OFFSET 0") }
        .to_return(status: 200, body: page_body([]))

      pages = []
      build_connection.paginate("t", "SELECT * FROM t", page_size: 2).each_page { |p| pages << p }
      expect(pages).to be_empty
    end

    it "yields a single page when fewer rows than page_size" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("LIMIT 10 OFFSET 0") }
        .to_return(status: 200, body: page_body([[1, "a"], [2, "b"]]))

      pages = []
      build_connection.paginate("t", "SELECT * FROM t", page_size: 10).each_page { |p| pages << p }
      expect(pages.size).to eq 1
      expect(pages.first.result_table.row_count).to eq 2
    end

    it "fetches subsequent pages until a short page" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("LIMIT 2 OFFSET 0") }
        .to_return(status: 200, body: page_body([[1, "a"], [2, "b"]]))

      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("LIMIT 2 OFFSET 2") }
        .to_return(status: 200, body: page_body([[3, "c"]]))

      pages = []
      build_connection.paginate("t", "SELECT * FROM t", page_size: 2).each_page { |p| pages << p }
      expect(pages.size).to eq 2
      expect(pages[0].result_table.row_count).to eq 2
      expect(pages[1].result_table.row_count).to eq 1
    end

    it "stops after exactly full pages then empty page (no extra request)" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("LIMIT 2 OFFSET 0") }
        .to_return(status: 200, body: page_body([[1, "a"], [2, "b"]]))

      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("LIMIT 2 OFFSET 2") }
        .to_return(status: 200, body: page_body([]))

      pages = []
      build_connection.paginate("t", "SELECT * FROM t", page_size: 2).each_page { |p| pages << p }
      # empty page is not yielded, only the first full page
      expect(pages.size).to eq 1
    end

    it "returns an Enumerator when called without a block" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: page_body([[1, "a"]]))

      enum = build_connection.paginate("t", "SELECT * FROM t", page_size: 10).each_page
      expect(enum).to be_a(Enumerator)
      pages = enum.to_a
      expect(pages.size).to eq 1
    end

    it "appends LIMIT/OFFSET to the base query correctly" do
      expected_sql = "SELECT id, name FROM t ORDER BY id LIMIT 5 OFFSET 0"
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"] == expected_sql }
        .to_return(status: 200, body: page_body([[1, "x"]]))

      build_connection
        .paginate("t", "SELECT id, name FROM t ORDER BY id", page_size: 5)
        .each_page {}
    end
  end

  describe "#each / #each_row" do
    it "yields individual rows across all pages" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("OFFSET 0") }
        .to_return(status: 200, body: page_body([[1, "a"], [2, "b"]]))

      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("OFFSET 2") }
        .to_return(status: 200, body: page_body([[3, "c"]]))

      rows = []
      build_connection.paginate("t", "SELECT * FROM t", page_size: 2).each_row { |r| rows << r }
      expect(rows.size).to eq 3
      expect(rows.map { |r| r[0].to_s.to_i }).to eq [1, 2, 3]
    end

    it "returns an Enumerator when called without a block" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: page_body([[1, "a"]]))

      enum = build_connection.paginate("t", "SELECT * FROM t", page_size: 10).each_row
      expect(enum).to be_a(Enumerator)
    end

    it "each is an alias for each_row" do
      paginator = build_connection.paginate("t", "SELECT * FROM t", page_size: 10)
      expect(paginator.method(:each)).to eq paginator.method(:each_row)
    end
  end

  describe "Enumerable support" do
    before do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["sql"].include?("OFFSET 0") }
        .to_return(status: 200, body: page_body([[1, "a"], [2, "b"], [3, "c"]]))
    end

    let(:paginator) { build_connection.paginate("t", "SELECT * FROM t", page_size: 10) }

    it "supports .map" do
      ids = paginator.map { |row| row[0].to_s.to_i }
      expect(ids).to eq [1, 2, 3]
    end

    it "supports .select" do
      even = paginator.select { |row| row[0].to_s.to_i.even? }
      expect(even.size).to eq 1
    end

    it "supports .to_a" do
      expect(paginator.to_a.size).to eq 3
    end

    it "supports .count" do
      expect(paginator.count).to eq 3
    end

    it "supports .first" do
      expect(paginator.first[0].to_s).to eq "1"
    end
  end

  describe "per-query timeout forwarding" do
    it "passes query_timeout_ms to execute_sql" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=3000") }
        .to_return(status: 200, body: page_body([[1, "a"]]))

      build_connection
        .paginate("t", "SELECT * FROM t", page_size: 10, query_timeout_ms: 3000)
        .each_page {}
    end
  end

  describe "Connection#paginate factory" do
    it "returns a Paginator" do
      expect(build_connection.paginate("t", "SELECT * FROM t")).to be_a(Pinot::Paginator)
    end

    it "defaults page_size to DEFAULT_PAGE_SIZE" do
      p = build_connection.paginate("t", "SELECT * FROM t")
      expect(p.instance_variable_get(:@page_size)).to eq Pinot::Paginator::DEFAULT_PAGE_SIZE
    end
  end
end
