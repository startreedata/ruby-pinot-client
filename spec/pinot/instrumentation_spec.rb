RSpec.describe Pinot::Instrumentation do
  before do
    # Reset the callback between tests
    described_class.on_query = nil
  end

  after do
    described_class.on_query = nil
  end

  describe ".on_query" do
    it "is nil by default" do
      expect(described_class.on_query).to be_nil
    end

    it "can be set to a callback" do
      cb = proc { |_event| }
      described_class.on_query = cb
      expect(described_class.on_query).to equal(cb)
    end
  end

  describe ".instrument" do
    it "calls the callback with correct event keys" do
      received = nil
      described_class.on_query = proc { |event| received = event }

      described_class.instrument(table: "myTable", query: "SELECT 1") { :ok }

      expect(received).to include(
        :table, :query, :duration_ms, :success, :error
      )
    end

    it "sets success: true on success" do
      received = nil
      described_class.on_query = proc { |event| received = event }

      described_class.instrument(table: "myTable", query: "SELECT 1") { :ok }

      expect(received[:success]).to be true
      expect(received[:error]).to be_nil
      expect(received[:table]).to eq("myTable")
      expect(received[:query]).to eq("SELECT 1")
    end

    it "sets success: false and error on failure and re-raises" do
      received = nil
      described_class.on_query = proc { |event| received = event }

      error = RuntimeError.new("boom")
      expect do
        described_class.instrument(table: "t", query: "q") { raise error }
      end.to raise_error(RuntimeError, "boom")

      expect(received[:success]).to be false
      expect(received[:error]).to equal(error)
    end

    it "duration_ms is a positive Float" do
      received = nil
      described_class.on_query = proc { |event| received = event }

      described_class.instrument(table: "t", query: "q") { :result }

      expect(received[:duration_ms]).to be_a(Float)
      expect(received[:duration_ms]).to be >= 0
    end

    it "does not call callback when on_query is nil" do
      described_class.on_query = nil
      expect do
        described_class.instrument(table: "t", query: "q") { :ok }
      end.not_to raise_error
    end

    it "returns the block's return value" do
      described_class.on_query = nil
      result = described_class.instrument(table: "t", query: "q") { 42 }
      expect(result).to eq(42)
    end
  end
end
