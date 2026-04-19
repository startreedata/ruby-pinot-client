module Pinot
  module Instrumentation
    # Called around every query execution.
    # Implement by setting Pinot::Instrumentation.on_query = proc { |event| ... }
    # event is a Hash:
    #   :table        => String
    #   :query        => String
    #   :duration_ms  => Float
    #   :success      => Boolean
    #   :error        => Exception or nil

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
