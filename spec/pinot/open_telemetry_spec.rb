require "pinot/open_telemetry"

# ---------------------------------------------------------------------------
# Minimal OpenTelemetry stub — no opentelemetry-api gem needed.
# ---------------------------------------------------------------------------
module OpenTelemetry
  class FakeSpan
    attr_reader :name, :attributes, :kind, :events, :status_set

    def initialize(name, attributes, kind)
      @name       = name
      @attributes = attributes
      @kind       = kind
      @events     = []
      @status_set = nil
    end

    def record_exception(ex)
      @events << { exception: ex }
    end

    def status=(s)
      @status_set = s
    end

    module Status
      def self.error(msg)
        { code: :error, message: msg }
      end
    end

    module Trace
      Status = OpenTelemetry::FakeSpan::Status
    end
  end

  class FakeTracer
    attr_reader :spans

    def initialize
      @spans = []
    end

    def in_span(name, attributes: {}, kind: :internal)
      span = FakeSpan.new(name, attributes, kind)
      @spans << span
      yield span
    end
  end

  class FakeTracerProvider
    attr_reader :tracer

    def initialize
      @tracer = FakeTracer.new
    end

    def tracer(_name = nil, _version = nil)
      @tracer
    end
  end

  class FakePropagator
    attr_accessor :injected_carriers

    def initialize
      @injected_carriers = []
    end

    def inject(carrier)
      carrier["traceparent"] = "00-abcdef1234567890-1234567890abcdef-01"
      carrier["tracestate"]  = "vendor=data"
      @injected_carriers << carrier
    end
  end

  @tracer_provider = FakeTracerProvider.new
  @propagation     = FakePropagator.new

  def self.tracer_provider
    @tracer_provider
  end

  def self.propagation
    @propagation
  end

  def self.reset!
    @tracer_provider = FakeTracerProvider.new
    @propagation     = FakePropagator.new
  end

  Trace = FakeSpan::Trace
end

# ---------------------------------------------------------------------------

RSpec.describe Pinot::OpenTelemetry do
  def tracer   = OpenTelemetry.tracer_provider.tracer
  def spans    = tracer.spans
  def prop     = OpenTelemetry.propagation

  before do
    OpenTelemetry.reset!
    described_class.uninstall!
    Pinot::Instrumentation.around    = nil
    Pinot::Instrumentation.on_query  = nil
    described_class.enabled = true
  end

  after do
    described_class.uninstall!
    Pinot::Instrumentation.around   = nil
    Pinot::Instrumentation.on_query = nil
  end

  # ---- install / uninstall --------------------------------------------------

  describe ".install!" do
    it "registers an around hook" do
      described_class.install!
      expect(Pinot::Instrumentation.around).not_to be_nil
    end

    it "is idempotent — calling install! twice does not raise" do
      expect do
        described_class.install!
        described_class.install!
      end.not_to raise_error
    end

    it "marks itself as installed" do
      described_class.install!
      expect(described_class).to be_installed
    end

    it "prepends TraceContextInjector into JsonHttpTransport" do
      described_class.install!
      expect(Pinot::JsonHttpTransport.ancestors).to include(Pinot::OpenTelemetry::TraceContextInjector)
    end
  end

  describe ".uninstall!" do
    it "removes the around hook" do
      described_class.install!
      described_class.uninstall!
      expect(Pinot::Instrumentation.around).to be_nil
    end

    it "marks itself as not installed" do
      described_class.install!
      described_class.uninstall!
      expect(described_class).not_to be_installed
    end
  end

  # ---- enabled flag ---------------------------------------------------------

  describe ".enabled= / .enabled?" do
    it "is enabled by default" do
      described_class.install!
      expect(described_class).to be_enabled
    end

    it "can be disabled at runtime" do
      described_class.install!
      described_class.enabled = false
      expect(described_class).not_to be_enabled
    end

    it "does not create spans when disabled" do
      described_class.install!
      described_class.enabled = false

      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :result }

      expect(spans).to be_empty
    end

    it "still fires post-query listeners when disabled" do
      described_class.install!
      described_class.enabled = false
      events = []
      Pinot::Instrumentation.subscribe(->(e) { events << e })

      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :result }

      expect(events.size).to eq 1
      expect(events.first[:success]).to be true
    end

    it "resumes creating spans after re-enabling" do
      described_class.install!
      described_class.enabled = false
      described_class.enabled = true

      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :result }

      expect(spans.size).to eq 1
    end
  end

  # ---- span creation --------------------------------------------------------

  describe "span attributes" do
    before { described_class.install! }

    def run(table: "orders", query: "SELECT count(*) FROM orders")
      Pinot::Instrumentation.instrument(table: table, query: query) { :ok }
    end

    it "creates a span named 'pinot.query'" do
      run
      expect(spans.first.name).to eq "pinot.query"
    end

    it "sets db.system to 'pinot'" do
      run
      expect(spans.first.attributes["db.system"]).to eq "pinot"
    end

    it "sets db.statement to the SQL string" do
      run(query: "SELECT * FROM orders")
      expect(spans.first.attributes["db.statement"]).to eq "SELECT * FROM orders"
    end

    it "sets db.name to the table name" do
      run(table: "orders")
      expect(spans.first.attributes["db.name"]).to eq "orders"
    end

    it "sets db.operation to the first SQL token uppercased" do
      run(query: "select count(*) FROM orders")
      expect(spans.first.attributes["db.operation"]).to eq "SELECT"
    end

    it "sets db.operation for INSERT queries" do
      run(query: "INSERT INTO t VALUES (1)")
      expect(spans.first.attributes["db.operation"]).to eq "INSERT"
    end

    it "uses kind :client" do
      run
      expect(spans.first.kind).to eq :client
    end

    it "creates one span per query" do
      3.times { run }
      expect(spans.size).to eq 3
    end
  end

  # ---- success / failure path -----------------------------------------------

  describe "span status on success" do
    before { described_class.install! }

    it "does not set error status on a successful query" do
      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :ok }
      expect(spans.first.status_set).to be_nil
    end

    it "does not record an exception on success" do
      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :ok }
      expect(spans.first.events).to be_empty
    end
  end

  describe "span status on failure" do
    before { described_class.install! }

    it "records the exception on the span" do
      err = Pinot::QueryTimeoutError.new("timeout")
      expect do
        Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { raise err }
      end.to raise_error(Pinot::QueryTimeoutError)
      expect(spans.first.events.first[:exception]).to be err
    end

    it "sets span status to error with the exception message" do
      err = RuntimeError.new("boom")
      begin
        Pinot::Instrumentation.instrument(table: "t", query: "Q") { raise err }
      rescue StandardError
        nil
      end
      expect(spans.first.status_set).to eq({ code: :error, message: "boom" })
    end

    it "re-raises the original exception" do
      expect do
        Pinot::Instrumentation.instrument(table: "t", query: "Q") { raise Pinot::TransportError, "down" }
      end.to raise_error(Pinot::TransportError, "down")
    end
  end

  # ---- post-query listeners are still notified ------------------------------

  describe "Instrumentation.notify still fires" do
    before { described_class.install! }

    it "fires subscribed listeners on success" do
      events = []
      Pinot::Instrumentation.subscribe(->(e) { events << e })
      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :ok }
      expect(events.size).to eq 1
      expect(events.first[:success]).to be true
      expect(events.first[:table]).to eq "t"
    end

    it "fires subscribed listeners on failure" do
      events = []
      Pinot::Instrumentation.subscribe(->(e) { events << e })
      begin
        Pinot::Instrumentation.instrument(table: "t", query: "Q") { raise "err" }
      rescue StandardError
        nil
      end
      expect(events.first[:success]).to be false
      expect(events.first[:error]).to be_a(RuntimeError)
    end

    it "works alongside a second post-query listener" do
      extra_events = []
      listener = Pinot::Instrumentation.subscribe(->(e) { extra_events << e })

      Pinot::Instrumentation.instrument(table: "t", query: "SELECT 1") { :ok }

      expect(spans.size).to eq 1
      expect(extra_events.size).to eq 1
      expect(extra_events.first[:table]).to eq "t"
    ensure
      Pinot::Instrumentation.unsubscribe(listener)
    end
  end

  # ---- TraceContextInjector -------------------------------------------------

  describe "TraceContextInjector" do
    let(:otel) { described_class }
    let(:sql_response) do
      '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[1]]},' \
        '"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":2}'
    end

    def build_conn
      selector  = Pinot::SimpleBrokerSelector.new(["localhost:8000"])
      transport = Pinot::JsonHttpTransport.new(http_client: Pinot::HttpClient.new, extra_headers: {})
      conn      = Pinot::Connection.new(transport: transport, broker_selector: selector)
      selector.init
      conn
    end

    before { otel.install! }

    it "injects traceparent into the outbound request" do
      stub = stub_request(:post, "http://localhost:8000/query/sql")
               .to_return(status: 200, body: sql_response)

      build_conn.execute_sql("orders", "SELECT count(*) FROM orders")

      expect(stub).to have_been_requested
      expect(a_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "traceparent" => "00-abcdef1234567890-1234567890abcdef-01" }))
        .to have_been_made
    end

    it "injects tracestate into the outbound request" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      build_conn.execute_sql("orders", "SELECT 1")

      expect(a_request(:post, "http://localhost:8000/query/sql")
        .with(headers: { "tracestate" => "vendor=data" }))
        .to have_been_made
    end

    it "does not inject trace headers when disabled" do
      otel.enabled = false
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      build_conn.execute_sql("orders", "SELECT 1")

      expect(prop.injected_carriers).to be_empty
    end

    it "does not inject headers when OTel is uninstalled" do
      otel.uninstall!
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      build_conn.execute_sql("orders", "SELECT 1")

      expect(prop.injected_carriers).to be_empty
    end
  end
end
