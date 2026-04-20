require "pinot/active_support_notifications"

# Minimal ActiveSupport::Notifications stub — no activesupport gem needed.
module ActiveSupport
  module Notifications
    @events = []

    def self.instrument(name, payload = {})
      start  = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result = block_given? ? yield : nil
      finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @events << { name: name, payload: payload, duration: finish - start }
      result
    end

    def self.recorded_events
      @events
    end

    def self.clear!
      @events = []
    end
  end
end

RSpec.describe Pinot::ActiveSupportNotifications do
  before do
    ActiveSupport::Notifications.clear!
    described_class.uninstall!
  end

  after do
    described_class.uninstall!
    ActiveSupport::Notifications.clear!
  end

  describe ".install!" do
    it "registers an on_query callback" do
      described_class.install!
      expect(Pinot::Instrumentation.on_query).not_to be_nil
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
  end

  describe ".uninstall!" do
    it "removes the on_query callback" do
      described_class.install!
      described_class.uninstall!
      expect(Pinot::Instrumentation.on_query).to be_nil
    end

    it "marks itself as not installed" do
      described_class.install!
      described_class.uninstall!
      expect(described_class).not_to be_installed
    end
  end

  describe "event publishing" do
    before { described_class.install! }

    def fire(table: "myTable", query: "SELECT 1", duration_ms: 12.5, success: true, error: nil)
      Pinot::Instrumentation.notify(
        table: table, query: query, duration_ms: duration_ms, success: success, error: error
      )
    end

    it "publishes a 'sql.pinot' event to ActiveSupport::Notifications" do
      fire
      events = ActiveSupport::Notifications.recorded_events
      expect(events.size).to eq 1
      expect(events.first[:name]).to eq "sql.pinot"
    end

    it "includes :sql with the query string" do
      fire(query: "SELECT count(*) FROM orders")
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:sql]).to eq "SELECT count(*) FROM orders"
    end

    it "includes :name with the table name" do
      fire(table: "orders")
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:name]).to eq "orders"
    end

    it "includes :duration with the duration in ms" do
      fire(duration_ms: 42.7)
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:duration]).to eq 42.7
    end

    it "includes :success true for a successful query" do
      fire(success: true)
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:success]).to be true
    end

    it "omits :exception and :exception_object when there is no error" do
      fire(error: nil)
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload).not_to have_key(:exception)
      expect(payload).not_to have_key(:exception_object)
    end

    it "includes :success false for a failed query" do
      fire(success: false, error: RuntimeError.new("boom"))
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:success]).to be false
    end

    it "includes :exception as [ClassName, message] on error" do
      err = Pinot::QueryTimeoutError.new("timed out")
      fire(success: false, error: err)
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:exception]).to eq ["Pinot::QueryTimeoutError", "timed out"]
    end

    it "includes :exception_object as the raw exception" do
      err = Pinot::TransportError.new("down")
      fire(success: false, error: err)
      payload = ActiveSupport::Notifications.recorded_events.first[:payload]
      expect(payload[:exception_object]).to be err
    end

    it "publishes one event per query" do
      3.times { fire }
      expect(ActiveSupport::Notifications.recorded_events.size).to eq 3
    end

    it "does not publish events after uninstall!" do
      described_class.uninstall!
      fire
      expect(ActiveSupport::Notifications.recorded_events).to be_empty
    end
  end

  describe "end-to-end with Connection" do
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

    before { described_class.install! }

    it "publishes 'sql.pinot' when execute_sql succeeds" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 200, body: sql_response)

      build_conn.execute_sql("orders", "SELECT count(*) FROM orders")

      events = ActiveSupport::Notifications.recorded_events
      expect(events.size).to eq 1
      payload = events.first[:payload]
      expect(payload[:sql]).to eq "SELECT count(*) FROM orders"
      expect(payload[:name]).to eq "orders"
      expect(payload[:success]).to be true
    end

    it "publishes 'sql.pinot' with exception details when execute_sql raises" do
      stub_request(:post, "http://localhost:8000/query/sql")
        .to_return(status: 503, body: "")

      begin
        build_conn.execute_sql("orders", "SELECT 1")
      rescue StandardError
        nil
      end

      events = ActiveSupport::Notifications.recorded_events
      expect(events.size).to eq 1
      payload = events.first[:payload]
      expect(payload[:success]).to be false
      expect(payload[:exception]).to be_an(Array)
      expect(payload[:exception_object]).to be_a(Pinot::BrokerUnavailableError)
    end
  end
end
