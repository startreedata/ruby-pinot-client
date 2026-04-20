module Pinot
  # Per-broker circuit breaker implementing the classic three-state machine:
  #
  #   CLOSED    — normal operation; failures are counted
  #   OPEN      — all calls rejected immediately with BrokerCircuitOpenError
  #   HALF_OPEN — one probe call allowed through; success → CLOSED, failure → OPEN
  #
  # A breaker opens after +failure_threshold+ consecutive transport-level failures
  # (BrokerUnavailableError, connection resets, timeouts). It automatically
  # transitions to HALF_OPEN after +open_timeout+ seconds.
  #
  # Use CircuitBreakerRegistry to share breakers across Connection instances.
  #
  # == Configuration
  #
  #   config = Pinot::ClientConfig.new(
  #     broker_list:               ["broker:8099"],
  #     circuit_breaker_enabled:   true,
  #     circuit_breaker_threshold: 3,   # open after 3 failures (default 5)
  #     circuit_breaker_timeout:   10   # reopen probe after 10 s (default 30)
  #   )
  #   conn = Pinot.from_config(config)
  #
  # == Error class
  #
  #   Pinot::CircuitBreaker::BrokerCircuitOpenError
  #     — raised when the circuit is OPEN; inherits from BrokerNotFoundError
  #       so callers that already rescue BrokerNotFoundError get it for free.
  #
  # @param failure_threshold [Integer] consecutive failures before opening (default 5)
  # @param open_timeout      [Integer] seconds to wait before probing again (default 30)
  class CircuitBreaker
    CLOSED    = :closed
    OPEN      = :open
    HALF_OPEN = :half_open

    class BrokerCircuitOpenError < BrokerNotFoundError
    end

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
    def call(_broker_address)
      @mutex.synchronize { check_state! }
      begin
        result = yield
        @mutex.synchronize { on_success }
        result
      rescue BrokerUnavailableError, Errno::ECONNRESET, Errno::ECONNREFUSED,
             Errno::ETIMEDOUT, Net::OpenTimeout, Net::ReadTimeout, Net::WriteTimeout
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

  # Thread-safe registry that lazily creates and caches one CircuitBreaker per
  # broker address string. Shared by all Connection instances built from the
  # same ClientConfig so that failures from parallel queries accumulate correctly.
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
          open_timeout: @open_timeout
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
