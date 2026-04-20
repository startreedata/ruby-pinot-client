# Minimal Rails stubs — no rails gem needed.
module Rails
  class Railtie
    def self.initializer(name, &block)
      @initializers ||= {}
      @initializers[name] = block
    end

    def self.run_initializer(name, app)
      @initializers ||= {}
      @initializers[name]&.call(app)
    end

    def self.initializers
      @initializers || {}
    end
  end
end

module ActiveSupport
  class OrderedOptions < Hash
    def method_missing(name, *args)
      if name.to_s.end_with?("=")
        self[name.to_s.chomp("=").to_sym] = args.first
      else
        self[name]
      end
    end

    def respond_to_missing?(*) = true
  end
end

require "pinot/railtie"

RSpec.describe Pinot::Railtie do
  let(:app_config) do
    cfg = double("AppConfig")
    pinot_opts = ActiveSupport::OrderedOptions.new
    pinot_opts[:notifications]  = true
    pinot_opts[:open_telemetry] = true
    pinot_opts[:request_id]     = true
    allow(cfg).to receive_messages(pinot: pinot_opts, middleware: middleware_stack)
    cfg
  end

  let(:middleware_stack) do
    stack = []
    allow_obj = double("MiddlewareStack")
    allow(allow_obj).to receive(:use) { |klass| stack << klass }
    allow_obj.define_singleton_method(:stack) { stack }
    allow_obj
  end

  let(:app) do
    double("App", config: app_config)
  end

  before do
    # Reset state between examples
    Pinot::ActiveSupportNotifications.uninstall! if Pinot::ActiveSupportNotifications.installed?
    Pinot::Instrumentation.on_query = nil
  end

  after do
    Pinot::ActiveSupportNotifications.uninstall!
    Pinot::Instrumentation.on_query = nil
  end

  # ── initializer registration ─────────────────────────────────────────────────

  describe "initializer registration" do
    it "registers pinot.install_notifications" do
      expect(described_class.initializers).to have_key("pinot.install_notifications")
    end

    it "registers pinot.install_open_telemetry" do
      expect(described_class.initializers).to have_key("pinot.install_open_telemetry")
    end

    it "registers pinot.request_id_propagation" do
      expect(described_class.initializers).to have_key("pinot.request_id_propagation")
    end
  end

  # ── notifications initializer ────────────────────────────────────────────────

  describe "pinot.install_notifications" do
    it "installs the AS::N bridge when notifications: true" do
      described_class.run_initializer("pinot.install_notifications", app)
      expect(Pinot::ActiveSupportNotifications).to be_installed
    end

    it "skips the bridge when notifications: false" do
      app_config.pinot[:notifications] = false
      described_class.run_initializer("pinot.install_notifications", app)
      expect(Pinot::ActiveSupportNotifications).not_to be_installed
    end
  end

  # ── open_telemetry initializer ───────────────────────────────────────────────

  describe "pinot.install_open_telemetry" do
    it "skips silently when open_telemetry: false" do
      app_config.pinot[:open_telemetry] = false
      expect do
        described_class.run_initializer("pinot.install_open_telemetry", app)
      end.not_to raise_error
    end

    it "skips silently when opentelemetry gem is not available" do
      app_config.pinot[:open_telemetry] = true
      # Simulate LoadError for the opentelemetry require
      allow_any_instance_of(Object).to receive(:require).with("opentelemetry").and_raise(LoadError)
      expect do
        described_class.run_initializer("pinot.install_open_telemetry", app)
      end.not_to raise_error
    end
  end

  # ── request_id_propagation initializer ──────────────────────────────────────

  describe "pinot.request_id_propagation" do
    it "inserts RequestIdMiddleware into the middleware stack" do
      described_class.run_initializer("pinot.request_id_propagation", app)
      expect(middleware_stack.stack).to include(Pinot::RequestIdMiddleware)
    end

    it "prepends RequestIdInjector into Connection" do
      described_class.run_initializer("pinot.request_id_propagation", app)
      expect(Pinot::Connection.ancestors).to include(Pinot::RequestIdInjector)
    end

    it "skips when request_id: false" do
      app_config.pinot[:request_id] = false
      described_class.run_initializer("pinot.request_id_propagation", app)
      expect(middleware_stack.stack).not_to include(Pinot::RequestIdMiddleware)
    end
  end
end

# ── RequestIdMiddleware unit tests ───────────────────────────────────────────

RSpec.describe Pinot::RequestIdMiddleware do
  let(:app)        { ->(_env) { [200, {}, ["OK"]] } }
  let(:middleware) { described_class.new(app) }

  it "stores X-Request-Id in thread-local during the request" do
    captured = nil
    inner = lambda do |_env|
      captured = Thread.current[:pinot_request_id]
      [200, {}, ["OK"]]
    end
    described_class.new(inner).call("HTTP_X_REQUEST_ID" => "req-abc-123")
    expect(captured).to eq("req-abc-123")
  end

  it "clears the thread-local after the request" do
    middleware.call("HTTP_X_REQUEST_ID" => "req-xyz")
    expect(Thread.current[:pinot_request_id]).to be_nil
  end

  it "sets thread-local to nil when X-Request-Id header is absent" do
    captured = :unset
    inner = lambda do |_env|
      captured = Thread.current[:pinot_request_id]
      [200, {}, []]
    end
    described_class.new(inner).call({})
    expect(captured).to be_nil
  end

  it "clears thread-local even when the inner app raises" do
    exploding = ->(_env) { raise "boom" }
    begin
      described_class.new(exploding).call("HTTP_X_REQUEST_ID" => "req-1")
    rescue StandardError
      nil
    end
    expect(Thread.current[:pinot_request_id]).to be_nil
  end
end

# ── RequestIdInjector unit tests ─────────────────────────────────────────────

RSpec.describe Pinot::RequestIdInjector do
  let(:sql_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[1]]},' \
      '"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":2}'
  end

  def build_conn
    selector  = Pinot::SimpleBrokerSelector.new(["localhost:8000"])
    transport = Pinot::JsonHttpTransport.new(http_client: Pinot::HttpClient.new, extra_headers: {})
    conn      = Pinot::Connection.new(transport: transport, broker_selector: selector)
    conn.extend(Pinot::RequestIdInjector)
    selector.init
    conn
  end

  it "forwards X-Request-Id when thread-local is set" do
    stub = stub_request(:post, "http://localhost:8000/query/sql")
             .with(headers: { "X-Request-Id" => "req-forward-123" })
             .to_return(status: 200, body: sql_response)

    Thread.current[:pinot_request_id] = "req-forward-123"
    build_conn.execute_sql("t", "SELECT 1")
    Thread.current[:pinot_request_id] = nil

    expect(stub).to have_been_requested
  end

  it "does not add X-Request-Id when thread-local is nil" do
    stub_request(:post, "http://localhost:8000/query/sql")
      .to_return(status: 200, body: sql_response)

    Thread.current[:pinot_request_id] = nil
    build_conn.execute_sql("t", "SELECT 1")

    expect(a_request(:post, "http://localhost:8000/query/sql")
      .with(headers: { "X-Request-Id" => anything }))
      .not_to have_been_made
  end

  it "allows caller-supplied X-Request-Id to take precedence" do
    stub = stub_request(:post, "http://localhost:8000/query/sql")
             .with(headers: { "X-Request-Id" => "caller-wins" })
             .to_return(status: 200, body: sql_response)

    Thread.current[:pinot_request_id] = "thread-value"
    build_conn.execute_sql("t", "SELECT 1", headers: { "X-Request-Id" => "caller-wins" })
    Thread.current[:pinot_request_id] = nil

    expect(stub).to have_been_requested
  end
end
