RSpec.describe Pinot::JsonHttpTransport do
  let(:broker) { "localhost:8000" }
  let(:sql_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[97889]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":5}'
  end

  def build_transport(extra_headers: {}, timeout_ms: nil)
    Pinot::JsonHttpTransport.new(
      http_client: Pinot::HttpClient.new,
      extra_headers: extra_headers,
      timeout_ms: timeout_ms
    )
  end

  describe "#execute success" do
    it "returns a BrokerResponse" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response, headers: { "Content-Type" => "application/json" })

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      resp = transport.execute(broker, req)
      expect(resp).to be_a(Pinot::BrokerResponse)
      expect(resp.result_table.get_long(0, 0)).to eq 97889
    end
  end

  describe "#execute non-200" do
    it "raises on non-200 response" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 400, body: "")

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      expect { transport.execute(broker, req) }.to raise_error(Pinot::TransportError, /400/)
    end
  end

  describe "trace header" do
    it "includes trace=true in body when trace is true" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["trace"] == "true" }
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", true, false)
      transport.execute(broker, req)
    end

    it "does not include trace when false" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| !JSON.parse(r.body).key?("trace") }
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      transport.execute(broker, req)
    end
  end

  describe "query options" do
    it "includes useMultistageEngine=true when flag is set" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("useMultistageEngine=true") }
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", false, true)
      transport.execute(broker, req)
    end

    it "includes timeoutMs when timeout_ms is set" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| JSON.parse(r.body)["queryOptions"].to_s.include?("timeoutMs=5000") }
        .to_return(status: 200, body: sql_response)

      transport = build_transport(timeout_ms: 5000)
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      transport.execute(broker, req)
    end
  end

  describe "headers" do
    it "sets Content-Type header" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "Content-Type" => "application/json; charset=utf-8" })
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      transport.execute(broker, req)
    end

    it "sets X-Correlation-Id header" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with { |r| r.headers["X-Correlation-Id"].to_s.length > 0 }
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      transport.execute(broker, req)
    end

    it "forwards extra headers" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "X-Custom" => "value" })
        .to_return(status: 200, body: sql_response)

      transport = build_transport(extra_headers: { "X-Custom" => "value" })
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      transport.execute(broker, req)
    end
  end

  describe "URL building" do
    it "prepends http:// when no scheme" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "q", false, false)
      transport.execute("localhost:8000", req)
    end

    it "keeps https:// scheme" do
      stub_request(:post, "https://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("sql", "q", false, false)
      transport.execute("https://localhost:8000", req)
    end

    it "uses /query for pql format" do
      stub_request(:post, "http://localhost:8000/query")
        .to_return(status: 200, body: sql_response)

      transport = build_transport
      req = Pinot::Request.new("pql", "q", false, false)
      transport.execute("localhost:8000", req)
    end
  end

  describe "invalid JSON response" do
    it "raises on non-JSON body" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: "ProcessingException", headers: { "Content-Type" => "application/json" })

      transport = build_transport
      req = Pinot::Request.new("sql", "q", false, false)
      expect { transport.execute(broker, req) }.to raise_error(/unexpected token/)
    end
  end
end

RSpec.describe Pinot::HttpClient do
  describe "timeout configuration" do
    it "sets open_timeout, read_timeout, and write_timeout when timeout is provided" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: "{}")

      http_double = instance_double(Net::HTTP)
      allow(Net::HTTP).to receive(:new).and_return(http_double)
      allow(http_double).to receive(:use_ssl=)
      allow(http_double).to receive(:open_timeout=)
      allow(http_double).to receive(:read_timeout=)
      allow(http_double).to receive(:write_timeout=)
      allow(http_double).to receive(:request).and_return(
        instance_double(Net::HTTPResponse, code: "200", body: "{}")
      )

      client = Pinot::HttpClient.new(timeout: 5)
      client.post("http://localhost:8000/query/sql", body: "{}")

      expect(http_double).to have_received(:open_timeout=).with(5)
      expect(http_double).to have_received(:read_timeout=).with(5)
      expect(http_double).to have_received(:write_timeout=).with(5)
    end

    it "does not set any timeout when timeout is nil" do
      http_double = instance_double(Net::HTTP)
      allow(Net::HTTP).to receive(:new).and_return(http_double)
      allow(http_double).to receive(:use_ssl=)
      allow(http_double).to receive(:open_timeout=)
      allow(http_double).to receive(:read_timeout=)
      allow(http_double).to receive(:write_timeout=)
      allow(http_double).to receive(:request).and_return(
        instance_double(Net::HTTPResponse, code: "200", body: "{}")
      )

      client = Pinot::HttpClient.new
      client.post("http://localhost:8000/query/sql", body: "{}")

      expect(http_double).not_to have_received(:open_timeout=)
      expect(http_double).not_to have_received(:read_timeout=)
      expect(http_double).not_to have_received(:write_timeout=)
    end
  end
end
