RSpec.describe "Connection#healthy?" do
  let(:selector)  { Pinot::SimpleBrokerSelector.new(["localhost:8000"]) }
  let(:transport) { Pinot::JsonHttpTransport.new(http_client: Pinot::HttpClient.new, extra_headers: {}) }
  let(:conn)      { Pinot::Connection.new(transport: transport, broker_selector: selector) }

  before { selector.init }

  context "when /health returns 200" do
    before { stub_request(:get, "http://localhost:8000/health").to_return(status: 200, body: "OK") }

    it "returns true" do
      expect(conn.healthy?).to be true
    end

    it "returns true when called with an explicit table" do
      expect(conn.healthy?(table: "orders")).to be true
    end
  end

  context "when /health returns non-200" do
    before { stub_request(:get, "http://localhost:8000/health").to_return(status: 503, body: "") }

    it "returns false for 503" do
      expect(conn.healthy?).to be false
    end
  end

  context "when /health returns 404" do
    before { stub_request(:get, "http://localhost:8000/health").to_return(status: 404, body: "") }

    it "returns false for 404" do
      expect(conn.healthy?).to be false
    end
  end

  context "when the broker is unreachable" do
    before { stub_request(:get, "http://localhost:8000/health").to_raise(Errno::ECONNREFUSED) }

    it "returns false instead of raising" do
      expect(conn.healthy?).to be false
    end
  end

  context "when a timeout occurs" do
    before { stub_request(:get, "http://localhost:8000/health").to_raise(Net::OpenTimeout) }

    it "returns false instead of raising" do
      expect(conn.healthy?).to be false
    end
  end

  context "with a custom timeout" do
    before { stub_request(:get, "http://localhost:8000/health").to_return(status: 200, body: "OK") }

    it "accepts a timeout_ms keyword argument" do
      expect(conn.healthy?(timeout_ms: 500)).to be true
    end
  end

  context "when the broker selector raises" do
    let(:bad_selector) { Pinot::SimpleBrokerSelector.new([]) }
    let(:conn_no_broker) do
      Pinot::Connection.new(transport: transport, broker_selector: bad_selector)
    end

    it "returns false when no broker is available" do
      expect(conn_no_broker.healthy?).to be false
    end
  end
end
