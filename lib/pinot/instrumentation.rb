module Pinot
  # Low-level instrumentation hook that fires after every query executed via
  # Connection#execute_sql. This is the extension point used by
  # Pinot::ActiveSupportNotifications, Pinot::OpenTelemetry, and any custom
  # observability layer.
  #
  # == Subscribing (multiple listeners supported)
  #
  #   listener = Pinot::Instrumentation.subscribe(->(event) do
  #     MyMetrics.record(event[:table], event[:duration_ms], event[:success])
  #   end)
  #
  #   # Remove later:
  #   Pinot::Instrumentation.unsubscribe(listener)
  #
  # == Legacy single-callback API (still supported)
  #
  #   Pinot::Instrumentation.on_query = ->(event) { ... }
  #   Pinot::Instrumentation.on_query = nil  # remove
  #
  # == Around-execution hook (for OTel and similar span-based tools)
  #
  # The `around` hook wraps the entire query execution — the block yields the
  # query, and the hook is responsible for calling Instrumentation.notify when
  # done. Only one around hook can be registered at a time.
  #
  #   Pinot::Instrumentation.around = ->(table:, query:) do
  #     MyTracer.in_span("pinot") { yield }
  #   end
  #
  # == Event Hash keys
  #
  #   :table        => String  — table name passed to execute_sql
  #   :query        => String  — SQL string
  #   :duration_ms  => Float   — wall-clock time in milliseconds
  #   :success      => Boolean — false when an exception was raised
  #   :error        => Exception or nil — the exception on failure, nil on success
  module Instrumentation
    @listeners = []
    @around    = nil

    # Add a post-execution listener. Returns the listener so it can be passed
    # to unsubscribe later.
    def self.subscribe(listener)
      @listeners << listener
      listener
    end

    # Remove a previously subscribed listener.
    def self.unsubscribe(listener)
      @listeners.delete(listener)
    end

    # Register an around-execution wrapper. Only one wrapper is supported;
    # the new value replaces any previous one. Set to nil to remove.
    def self.around=(wrapper)
      @around = wrapper
    end

    def self.around
      @around
    end

    # Legacy single-callback setter. Replaces all listeners with the given
    # callback (or clears them when nil).
    def self.on_query=(callback)
      @listeners = callback ? [callback] : []
    end

    # Returns the first registered listener (legacy compat).
    def self.on_query
      @listeners.first
    end

    def self.instrument(table:, query:)
      if @around
        @around.call(table: table, query: query) { yield }
      else
        _timed_instrument(table: table, query: query) { yield }
      end
    end

    # Fire all registered listeners with an event hash. Called by the default
    # instrument path and by the OTel around wrapper.
    def self.notify(event)
      @listeners.each { |l| l.call(event) }
    end

    private_class_method def self._timed_instrument(table:, query:)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result = yield
      duration_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000
      notify(table: table, query: query, duration_ms: duration_ms, success: true, error: nil)
      result
    rescue => e
      duration_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000
      notify(table: table, query: query, duration_ms: duration_ms, success: false, error: e)
      raise
    end
  end
end
