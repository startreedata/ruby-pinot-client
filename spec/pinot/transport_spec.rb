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
      expect { transport.execute(broker, req) }.to raise_error(/unexpected (token|character)/i)
    end
  end

  describe "logging" do
    it "logs an error at error level on non-200 response" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 500, body: "")

      spy_logger = instance_double(Logger)
      allow(spy_logger).to receive(:debug)
      expect(spy_logger).to receive(:error).with(/500/)

      transport = Pinot::JsonHttpTransport.new(
        http_client: Pinot::HttpClient.new,
        logger: spy_logger
      )
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      expect { transport.execute(broker, req) }.to raise_error(/500/)
    end

    it "logs a debug message before the HTTP call" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      spy_logger = instance_double(Logger)
      expect(spy_logger).to receive(:debug).with(/localhost:8000/)

      transport = Pinot::JsonHttpTransport.new(
        http_client: Pinot::HttpClient.new,
        logger: spy_logger
      )
      req = Pinot::Request.new("sql", "select count(*) from t", false, false)
      transport.execute(broker, req)
    end
  end
end

RSpec.describe Pinot::HttpClient do
  let(:url) { "http://localhost:8000/query/sql" }

  def make_fake_http(response)
    http = double("Net::HTTP")
    allow(http).to receive(:use_ssl=)
    allow(http).to receive(:open_timeout=)
    allow(http).to receive(:read_timeout=)
    allow(http).to receive(:write_timeout=)
    allow(http).to receive(:keep_alive_timeout=)
    allow(http).to receive(:start).and_return(http)
    allow(http).to receive(:finish)
    allow(http).to receive(:request).and_return(response)
    http
  end

  def make_response(body = "ok")
    resp = double("Net::HTTPResponse")
    allow(resp).to receive(:code).and_return("200")
    allow(resp).to receive(:body).and_return(body)
    resp
  end

  describe "connection reuse" do
    it "reuses a single Net::HTTP connection across multiple requests" do
      stub_request(:post, url).to_return(status: 200, body: "ok")

      fake_http = make_fake_http(make_response)
      expect(Net::HTTP).to receive(:new).once.and_return(fake_http)

      client = Pinot::HttpClient.new
      3.times { client.post(url, body: '{"sql":"select 1"}', headers: {}) }
    end
  end

  describe "error discards connection" do
    it "creates a fresh connection after a request raises an error" do
      stub_request(:post, url).to_return(status: 200, body: "ok")

      first_http = double("Net::HTTP first")
      allow(first_http).to receive(:use_ssl=)
      allow(first_http).to receive(:start).and_return(first_http)
      allow(first_http).to receive(:finish)
      allow(first_http).to receive(:request).and_raise(Errno::ECONNRESET)

      second_http = make_fake_http(make_response)

      expect(Net::HTTP).to receive(:new).twice.and_return(first_http, second_http)

      client = Pinot::HttpClient.new

      expect { client.post(url, body: '{"sql":"select 1"}', headers: {}) }.to raise_error(Errno::ECONNRESET)
      client.post(url, body: '{"sql":"select 1"}', headers: {})
    end
  end

  describe "pool cap" do
    it "caps the pool at MAX_POOL_SIZE connections" do
      client = Pinot::HttpClient.new
      key = "localhost:8000"
      uri = URI.parse(url)

      # Checkin MAX_POOL_SIZE + 1 connections — the last one should be finished, not pooled
      n = Pinot::HttpClient::MAX_POOL_SIZE + 1
      connections = Array.new(n) { double("Net::HTTP", finish: nil) }
      connections.each { |http| client.send(:checkin, key, http) }

      pool_size = client.instance_variable_get(:@pool).values.map(&:size).sum
      expect(pool_size).to eq(Pinot::HttpClient::MAX_POOL_SIZE)

      # The overflow connection should have been finished
      expect(connections.last).to have_received(:finish)
    end
  end

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
      allow(http_double).to receive(:keep_alive_timeout=)
      allow(http_double).to receive(:start).and_return(http_double)
      allow(http_double).to receive(:finish)
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
      allow(http_double).to receive(:keep_alive_timeout=)
      allow(http_double).to receive(:start).and_return(http_double)
      allow(http_double).to receive(:finish)
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

  describe "TLS configuration" do
    describe "when tls_config has insecure_skip_verify: true" do
      it "configures Net::HTTP with VERIFY_NONE for HTTPS URLs" do
        tls = Pinot::TlsConfig.new(insecure_skip_verify: true)
        client = Pinot::HttpClient.new(tls_config: tls)

        mock_http = instance_double(Net::HTTP)
        allow(mock_http).to receive(:use_ssl=)
        allow(mock_http).to receive(:verify_mode=)
        allow(mock_http).to receive(:open_timeout=)
        allow(mock_http).to receive(:read_timeout=)
        allow(mock_http).to receive(:write_timeout=)
        allow(mock_http).to receive(:keep_alive_timeout=)
        allow(mock_http).to receive(:start).and_return(mock_http)
        allow(mock_http).to receive(:finish)

        mock_response = instance_double(Net::HTTPResponse, code: "200", body: "{}")
        allow(mock_http).to receive(:request).and_return(mock_response)

        allow(Net::HTTP).to receive(:new).and_return(mock_http)

        client.get("https://localhost:8000/query/sql")

        expect(mock_http).to have_received(:use_ssl=).with(true)
        expect(mock_http).to have_received(:verify_mode=).with(OpenSSL::SSL::VERIFY_NONE)
      end
    end

    describe "when tls_config is nil (default)" do
      it "does not apply SSL config for HTTP URLs" do
        client = Pinot::HttpClient.new

        mock_http = instance_double(Net::HTTP)
        allow(mock_http).to receive(:use_ssl=)
        allow(mock_http).to receive(:verify_mode=)
        allow(mock_http).to receive(:open_timeout=)
        allow(mock_http).to receive(:read_timeout=)
        allow(mock_http).to receive(:write_timeout=)
        allow(mock_http).to receive(:keep_alive_timeout=)
        allow(mock_http).to receive(:start).and_return(mock_http)
        allow(mock_http).to receive(:finish)

        mock_response = instance_double(Net::HTTPResponse, code: "200", body: "{}")
        allow(mock_http).to receive(:request).and_return(mock_response)

        allow(Net::HTTP).to receive(:new).and_return(mock_http)

        client.get("http://localhost:8000/query/sql")

        expect(mock_http).to have_received(:use_ssl=).with(false)
        expect(mock_http).not_to have_received(:verify_mode=)
      end
    end
  end
end
