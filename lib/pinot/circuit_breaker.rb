module Pinot
  # Per-broker circuit breaker. States: CLOSED (normal), OPEN (rejecting), HALF_OPEN (probing).
  #
  # failure_threshold  - consecutive failures before opening (default 5)
  # open_timeout       - seconds to stay OPEN before moving to HALF_OPEN (default 30)
  class CircuitBreaker
    CLOSED    = :closed
    OPEN      = :open
    HALF_OPEN = :half_open

    BrokerCircuitOpenError = Class.new(BrokerNotFoundError)

    attr_reader :state, :failure_count

    def initialize(failure_threshold: 5, open_timeout: 30)
      @failure_threshold = failure_threshold
      @open_timeout      = open_timeout
      @mutex             = Mutex.new
      @state             = CLOSED
      @failure_count     = 0
      @opened_at         = nil
    end

    # Call the block; record success/failure and enforce open-circuit rejection.
    def call(broker_address)
      @mutex.synchronize { check_state! }
      begin
        result = yield
        @mutex.synchronize { on_success }
        result
      rescue BrokerUnavailableError, Errno::ECONNRESET, Errno::ECONNREFUSED,
             Errno::ETIMEDOUT, Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout => e
        @mutex.synchronize { on_failure }
        raise
      end
    end

    def open?
      @mutex.synchronize { @state == OPEN }
    end

    def reset
      @mutex.synchronize do
        @state         = CLOSED
        @failure_count = 0
        @opened_at     = nil
      end
    end

    private

    def check_state!
      return if @state == CLOSED

      if @state == OPEN
        if elapsed_since_open >= @open_timeout
          @state = HALF_OPEN
        else
          raise BrokerCircuitOpenError, "circuit open for broker (#{remaining_open_time.ceil}s remaining)"
        end
      end
      # HALF_OPEN: allow the probe through
    end

    def on_success
      @state         = CLOSED
      @failure_count = 0
      @opened_at     = nil
    end

    def on_failure
      @failure_count += 1
      if @state == HALF_OPEN || @failure_count >= @failure_threshold
        @state     = OPEN
        @opened_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end

    def elapsed_since_open
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - @opened_at
    end

    def remaining_open_time
      @open_timeout - elapsed_since_open
    end
  end

  # Registry of per-broker CircuitBreakers, shared across transport calls.
  class CircuitBreakerRegistry
    def initialize(failure_threshold: 5, open_timeout: 30)
      @failure_threshold = failure_threshold
      @open_timeout      = open_timeout
      @breakers          = {}
      @mutex             = Mutex.new
    end

    def for(broker_address)
      @mutex.synchronize do
        @breakers[broker_address] ||= CircuitBreaker.new(
          failure_threshold: @failure_threshold,
          open_timeout:      @open_timeout
        )
      end
    end

    def open?(broker_address)
      @mutex.synchronize { @breakers[broker_address]&.open? || false }
    end

    # Remove all state (useful for testing).
    def reset_all
      @mutex.synchronize { @breakers.clear }
    end
  end
end
