module Pinot
  # Low-level instrumentation hook that fires after every query executed via
  # Connection#execute_sql. This is the extension point used by
  # Pinot::ActiveSupportNotifications and any custom observability layer.
  #
  # Register a single callback:
  #
  #   Pinot::Instrumentation.on_query = ->(event) do
  #     MyMetrics.record(event[:table], event[:duration_ms], event[:success])
  #   end
  #
  # The event Hash contains:
  #   :table        => String  — table name passed to execute_sql
  #   :query        => String  — SQL string
  #   :duration_ms  => Float   — wall-clock time in milliseconds
  #   :success      => Boolean — false when an exception was raised
  #   :error        => Exception or nil — the exception on failure, nil on success
  #
  # Only one callback can be registered at a time. Set on_query= to nil to remove it.
  module Instrumentation
    def self.on_query=(callback)
      @on_query = callback
    end

    def self.on_query
      @on_query
    end

    def self.instrument(table:, query:)
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

    def self.notify(event)
      @on_query&.call(event)
    end
  end
end
