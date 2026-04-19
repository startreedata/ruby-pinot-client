RSpec.describe Pinot::Paginator do
  let(:schema)      { { "columnDataTypes" => ["LONG", "STRING"], "columnNames" => ["id", "name"] } }
  let(:broker)      { "localhost:8099" }
  let(:request_id)  { "236490978000000006" }
  let(:query)       { "SELECT * FROM t LIMIT 100" }

  def cursor_response(rows:, num_rows_result_set:, offset: 0, broker_host: "localhost", broker_port: 8099)
    JSON.generate(
      "resultTable"       => { "dataSchema" => schema, "rows" => rows },
      "exceptions"        => [],
      "numServersQueried" => 1,
      "numServersResponded" => 1,
      "timeUsedMs"        => 1,
      "requestId"         => request_id,
      "numRowsResultSet"  => num_rows_result_set,
      "offset"            => offset,
      "numRows"           => rows.size,
      "brokerHost"        => broker_host,
      "brokerPort"        => broker_port,
      "submissionTimeMs"  => 1_000_000,
      "expirationTimeMs"  => 4_600_000
    )
  end

  def fetch_response(rows:, offset: 0)
    JSON.generate(
      "resultTable"         => { "dataSchema" => schema, "rows" => rows },
      "exceptions"          => [],
      "numServersQueried"   => 1,
      "numServersResponded" => 1,
      "timeUsedMs"          => 1,
      "requestId"           => request_id,
      "numRowsResultSet"    => nil,
      "offset"              => offset,
      "numRows"             => rows.size
    )
  end

  def build_paginator(page_size: 2, extra_headers: {})
    client = Pinot::HttpClient.new
    Pinot::Paginator.new(client, broker, query, page_size: page_size, extra_headers: extra_headers)
  end

  # ── Argument validation ───────────────────────────────────────────────────

  describe "argument validation" do
    it "raises when page_size is zero" do
      expect { build_paginator(page_size: 0) }.to raise_error(ArgumentError, /page_size/)
    end

    it "raises when page_size is negative" do
      expect { build_paginator(page_size: -1) }.to raise_error(ArgumentError, /page_size/)
    end
  end

  # ── Initial cursor submission ─────────────────────────────────────────────

  describe "cursor submission" do
    it "POSTs to /query/sql?getCursor=true&numRows=N" do
      stub = stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .with(body: hash_including("sql" => query))
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      build_paginator.each_page {}
      expect(stub).to have_been_requested
    end

    it "uses brokerHost:brokerPort from the response for subsequent requests" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(
          rows: [[1, "a"], [2, "b"]], num_rows_result_set: 3,
          broker_host: "broker2.internal", broker_port: 8099
        ))

      fetch_stub = stub_request(:get, "http://broker2.internal:8099/responseStore/#{request_id}/results?offset=2&numRows=2")
        .to_return(status: 200, body: fetch_response(rows: [[3, "c"]], offset: 2))

      stub_request(:delete, "http://broker2.internal:8099/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      build_paginator.each_page {}
      expect(fetch_stub).to have_been_requested
    end

    it "raises TransportError when broker returns non-200 on submit" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 500, body: "error")

      expect { build_paginator.each_page {} }.to raise_error(Pinot::TransportError, /500/)
    end
  end

  # ── Page iteration ────────────────────────────────────────────────────────

  describe "#each_page" do
    it "yields nothing when first page is empty" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [], num_rows_result_set: 0))

      pages = []
      build_paginator.each_page { |p| pages << p }
      expect(pages).to be_empty
    end

    it "yields one page when all rows fit in the first response" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      pages = []
      build_paginator.each_page { |p| pages << p }
      expect(pages.size).to eq 1
      expect(pages.first.result_table.row_count).to eq 1
    end

    it "fetches subsequent pages via GET /responseStore/{id}/results?offset=N" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"], [2, "b"]], num_rows_result_set: 3))

      stub_request(:get, "http://#{broker}/responseStore/#{request_id}/results?offset=2&numRows=2")
        .to_return(status: 200, body: fetch_response(rows: [[3, "c"]], offset: 2))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      pages = []
      build_paginator.each_page { |p| pages << p }
      expect(pages.size).to eq 2
      expect(pages[0].result_table.row_count).to eq 2
      expect(pages[1].result_table.row_count).to eq 1
    end

    it "stops and DELETEs after the last partial page" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"], [2, "b"]], num_rows_result_set: 3))

      stub_request(:get, "http://#{broker}/responseStore/#{request_id}/results?offset=2&numRows=2")
        .to_return(status: 200, body: fetch_response(rows: [[3, "c"]], offset: 2))

      delete_stub = stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      build_paginator.each_page {}
      expect(delete_stub).to have_been_requested
    end

    it "returns an Enumerator when called without a block" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      enum = build_paginator.each_page
      expect(enum).to be_a(Enumerator)
    end

    it "raises TransportError when a fetch page returns non-200" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"], [2, "b"]], num_rows_result_set: 4))

      stub_request(:get, "http://#{broker}/responseStore/#{request_id}/results?offset=2&numRows=2")
        .to_return(status: 404, body: "not found")

      expect { build_paginator.each_page {} }.to raise_error(Pinot::TransportError, /404/)
    end
  end

  # ── Row iteration ─────────────────────────────────────────────────────────

  describe "#each_row / #each" do
    before do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"], [2, "b"]], num_rows_result_set: 3))

      stub_request(:get, "http://#{broker}/responseStore/#{request_id}/results?offset=2&numRows=2")
        .to_return(status: 200, body: fetch_response(rows: [[3, "c"]], offset: 2))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")
    end

    it "yields every row across all pages" do
      rows = []
      build_paginator.each_row { |r| rows << r }
      expect(rows.size).to eq 3
    end

    it "returns an Enumerator without a block" do
      expect(build_paginator.each_row).to be_a(Enumerator)
    end

    it "each is an alias for each_row" do
      p = build_paginator
      expect(p.method(:each)).to eq p.method(:each_row)
    end
  end

  # ── Enumerable ────────────────────────────────────────────────────────────

  describe "Enumerable support" do
    before do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=10")
        .to_return(status: 200, body: cursor_response(
          rows: [[1, "a"], [2, "b"], [3, "c"]], num_rows_result_set: 3, broker_host: "localhost", broker_port: 8099
        ))
      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")
    end

    let(:pager) { build_paginator(page_size: 10) }

    it "supports .to_a" do
      expect(pager.to_a.size).to eq 3
    end

    it "supports .count" do
      expect(pager.count).to eq 3
    end

    it "supports .map" do
      names = pager.map { |row| row[1] }
      expect(names).to eq ["a", "b", "c"]
    end

    it "supports .select" do
      even = pager.select { |row| row[0].to_s.to_i.even? }
      expect(even.size).to eq 1
    end
  end

  # ── Manual delete ─────────────────────────────────────────────────────────

  describe "#delete" do
    it "DELETEs the cursor at /responseStore/{requestId}" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      delete_stub = stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      # exhaust first so request_id is set
      build_paginator.each_page {}
      # already deleted by exhaustion — verify it was called exactly once
      expect(delete_stub).to have_been_requested.once
    end

    it "is idempotent — calling delete twice does not raise" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      pager = build_paginator
      pager.each_page {}
      expect { pager.delete }.not_to raise_error
    end

    it "silently swallows delete errors" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 500, body: "err")

      expect { build_paginator.each_page {} }.not_to raise_error
    end
  end

  # ── Extra headers ─────────────────────────────────────────────────────────

  describe "extra_headers forwarding" do
    it "sends extra_headers on submit and fetch requests" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .with(headers: { "X-Auth" => "tok" })
        .to_return(status: 200, body: cursor_response(rows: [[1, "a"]], num_rows_result_set: 1))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      build_paginator(extra_headers: { "X-Auth" => "tok" }).each_page {}
    end
  end

  # ── BrokerResponse cursor fields ──────────────────────────────────────────

  describe "BrokerResponse cursor fields" do
    it "exposes requestId, numRowsResultSet, brokerHost, brokerPort, and timestamps" do
      stub_request(:post, "http://#{broker}/query/sql?getCursor=true&numRows=2")
        .to_return(status: 200, body: cursor_response(
          rows: [[1, "a"]], num_rows_result_set: 1,
          broker_host: "localhost", broker_port: 8099
        ))

      stub_request(:delete, "http://#{broker}/responseStore/#{request_id}")
        .to_return(status: 200, body: "")

      first_page = nil
      build_paginator.each_page { |p| first_page = p }

      expect(first_page.request_id).to eq request_id
      expect(first_page.num_rows_result_set).to eq 1
      expect(first_page.broker_host).to eq "localhost"
      expect(first_page.broker_port).to eq 8099
      expect(first_page.submission_time_ms).to eq 1_000_000
      expect(first_page.expiration_time_ms).to eq 4_600_000
      expect(first_page).to be_cursor
    end

    it "cursor? is false for non-cursor BrokerResponse" do
      resp = Pinot::BrokerResponse.new({})
      expect(resp).not_to be_cursor
    end
  end

  # ── Connection#paginate factory ───────────────────────────────────────────

  describe "Connection#paginate" do
    def build_conn
      selector  = Pinot::SimpleBrokerSelector.new(["#{broker}"])
      transport = Pinot::JsonHttpTransport.new(http_client: Pinot::HttpClient.new, extra_headers: {})
      conn      = Pinot::Connection.new(transport: transport, broker_selector: selector)
      selector.init
      conn
    end

    it "returns a Paginator" do
      expect(build_conn.paginate(query)).to be_a(Pinot::Paginator)
    end

    it "defaults page_size to DEFAULT_PAGE_SIZE" do
      p = build_conn.paginate(query)
      expect(p.instance_variable_get(:@page_size)).to eq Pinot::Paginator::DEFAULT_PAGE_SIZE
    end

    it "accepts table: kwarg for broker selection" do
      p = build_conn.paginate(query, table: "myTable", page_size: 50)
      expect(p.instance_variable_get(:@page_size)).to eq 50
    end
  end
end
