RSpec.describe Pinot::CircuitBreaker do
  let(:breaker) { described_class.new(failure_threshold: 3, open_timeout: 1) }

  def cause_failure(n = 1)
    n.times do
      breaker.call("broker") { raise Pinot::BrokerUnavailableError, "down" }
    rescue Pinot::BrokerUnavailableError
      # expected
    end
  end

  describe "#state" do
    it "starts CLOSED" do
      expect(breaker.state).to eq :closed
    end
  end

  describe "#call" do
    it "passes through a successful call" do
      result = breaker.call("broker") { 42 }
      expect(result).to eq 42
      expect(breaker.state).to eq :closed
    end

    it "increments failure_count on BrokerUnavailableError" do
      cause_failure(1)
      expect(breaker.failure_count).to eq 1
      expect(breaker.state).to eq :closed
    end

    it "opens after failure_threshold consecutive failures" do
      cause_failure(3)
      expect(breaker.state).to eq :open
    end

    it "raises BrokerCircuitOpenError when OPEN" do
      cause_failure(3)
      expect { breaker.call("broker") { 1 } }.to raise_error(Pinot::CircuitBreaker::BrokerCircuitOpenError)
    end

    it "resets failure_count on success" do
      cause_failure(2)
      breaker.call("broker") { "ok" }
      expect(breaker.failure_count).to eq 0
      expect(breaker.state).to eq :closed
    end

    it "counts Errno::ECONNREFUSED as a failure" do
      begin
        breaker.call("broker") { raise Errno::ECONNREFUSED }
      rescue Errno::ECONNREFUSED
      end
      expect(breaker.failure_count).to eq 1
    end

    it "does not count non-network errors as failures" do
      begin
        breaker.call("broker") { raise ArgumentError, "bad" }
      rescue ArgumentError
      end
      expect(breaker.failure_count).to eq 0
      expect(breaker.state).to eq :closed
    end
  end

  describe "HALF_OPEN state" do
    before do
      cause_failure(3)
      sleep 1.05
    end

    it "transitions to HALF_OPEN after open_timeout" do
      breaker.call("broker") { "probe" } rescue nil
      # After open_timeout we allowed the probe through; success closes it
      expect(breaker.state).to eq :closed
    end

    it "re-opens on probe failure" do
      begin
        breaker.call("broker") { raise Pinot::BrokerUnavailableError, "still down" }
      rescue Pinot::BrokerUnavailableError
      end
      expect(breaker.state).to eq :open
    end
  end

  describe "#reset" do
    it "clears state to CLOSED" do
      cause_failure(3)
      breaker.reset
      expect(breaker.state).to eq :closed
      expect(breaker.failure_count).to eq 0
    end
  end

  describe "#open?" do
    it "returns false when CLOSED" do
      expect(breaker.open?).to be false
    end

    it "returns true when OPEN" do
      cause_failure(3)
      expect(breaker.open?).to be true
    end
  end
end

RSpec.describe Pinot::CircuitBreakerRegistry do
  let(:registry) { described_class.new(failure_threshold: 2, open_timeout: 60) }

  describe "#for" do
    it "returns a CircuitBreaker for a broker address" do
      cb = registry.for("broker1:8000")
      expect(cb).to be_a(Pinot::CircuitBreaker)
    end

    it "returns the same instance for the same address" do
      expect(registry.for("broker1:8000")).to be(registry.for("broker1:8000"))
    end

    it "returns different instances for different addresses" do
      expect(registry.for("broker1:8000")).not_to be(registry.for("broker2:8000"))
    end
  end

  describe "#open?" do
    it "returns false for an unknown broker" do
      expect(registry.open?("unknown:9999")).to be false
    end

    it "returns true after the circuit opens" do
      cb = registry.for("broker1:8000")
      2.times do
        cb.call("broker1:8000") { raise Pinot::BrokerUnavailableError }
      rescue Pinot::BrokerUnavailableError
      end
      expect(registry.open?("broker1:8000")).to be true
    end
  end

  describe "#reset_all" do
    it "clears all breakers" do
      cb = registry.for("broker1:8000")
      2.times do
        cb.call("broker1:8000") { raise Pinot::BrokerUnavailableError }
      rescue Pinot::BrokerUnavailableError
      end
      registry.reset_all
      expect(registry.open?("broker1:8000")).to be false
    end
  end
end

RSpec.describe "Circuit breaker integration via Connection" do
  let(:sql_response) do
    '{"resultTable":{"dataSchema":{"columnDataTypes":["LONG"],"columnNames":["cnt"]},"rows":[[1]]},"exceptions":[],"numServersQueried":1,"numServersResponded":1,"timeUsedMs":1}'
  end

  it "is disabled by default (no registry)" do
    stub_request(:post, "http://localhost:8000/query/sql")
      .to_return(status: 200, body: sql_response)

    conn = Pinot.from_broker_list(["localhost:8000"])
    expect(conn.instance_variable_get(:@circuit_breaker_registry)).to be_nil
    expect { conn.execute_sql("t", "select 1") }.not_to raise_error
  end

  it "wires CircuitBreakerRegistry when circuit_breaker_enabled is true" do
    config = Pinot::ClientConfig.new(
      broker_list: ["localhost:8000"],
      circuit_breaker_enabled: true,
      circuit_breaker_threshold: 3,
      circuit_breaker_timeout: 10
    )
    conn = Pinot.from_config(config)
    registry = conn.instance_variable_get(:@circuit_breaker_registry)
    expect(registry).to be_a(Pinot::CircuitBreakerRegistry)
  end

  it "raises BrokerCircuitOpenError after threshold failures" do
    stub_request(:post, "http://localhost:8000/query/sql")
      .to_return(status: 503, body: "unavailable")

    config = Pinot::ClientConfig.new(
      broker_list: ["localhost:8000"],
      circuit_breaker_enabled: true,
      circuit_breaker_threshold: 2
    )
    conn = Pinot.from_config(config)

    2.times do
      conn.execute_sql("t", "select 1")
    rescue Pinot::BrokerUnavailableError
    end

    expect { conn.execute_sql("t", "select 1") }
      .to raise_error(Pinot::CircuitBreaker::BrokerCircuitOpenError)
  end
end
