require "pinot"

module Pinot
  # Opt-in OpenTelemetry bridge.
  #
  # == Setup
  #
  # Add to an initializer after the opentelemetry SDK is configured:
  #
  #   require "pinot/open_telemetry"
  #   Pinot::OpenTelemetry.install!
  #
  # == What it does
  #
  # Each call to Connection#execute_sql (and anything built on top of it —
  # execute_sql_with_params, execute_many, PreparedStatement) creates an OTel
  # span named "pinot.query" with attributes following the OpenTelemetry
  # semantic conventions for database spans:
  #
  #   db.system        = "pinot"
  #   db.statement     = "<sql>"        (the full SQL string)
  #   db.name          = "<table>"      (the Pinot table name)
  #   db.operation     = "SELECT"       (first token of the SQL)
  #
  # On failure the span is marked with error status and the exception is
  # recorded on the span.
  #
  # == Trace-context propagation
  #
  # When installed, every outbound HTTP request to a broker is injected with
  # W3C Trace Context headers (traceparent / tracestate) so distributed traces
  # flow through Pinot. This relies on OpenTelemetry.propagation being
  # configured (the default SDK sets this up automatically).
  #
  # == Feature flag
  #
  # The bridge can be toggled at runtime without reinstalling:
  #
  #   Pinot::OpenTelemetry.enabled = false   # pause tracing (e.g. in tests)
  #   Pinot::OpenTelemetry.enabled = true    # resume
  #   Pinot::OpenTelemetry.enabled?          # => true / false
  #
  # == Lifecycle
  #
  #   Pinot::OpenTelemetry.install!    # idempotent
  #   Pinot::OpenTelemetry.installed?  # => true
  #   Pinot::OpenTelemetry.uninstall!  # removes hooks; leaves transport unpatched
  #
  # Note: this gem does NOT depend on opentelemetry-api or opentelemetry-sdk.
  # Both must be present and initialized before install! is called.
  module OpenTelemetry
    SPAN_NAME    = "pinot.query".freeze
    DB_SYSTEM    = "pinot".freeze
    TRACER_NAME  = "pinot-client".freeze

    @installed = false
    @enabled   = true

    # Enable or disable tracing at runtime without uninstalling.
    def self.enabled=(val)
      @enabled = val ? true : false
    end

    def self.enabled?
      @enabled
    end

    def self.install!
      return if installed?

      _install_around_hook
      _patch_transport
      @installed = true
    end

    def self.installed?
      @installed
    end

    def self.uninstall!
      ::Pinot::Instrumentation.around = nil
      @installed = false
      # NOTE: JsonHttpTransport prepend is permanent once applied (Ruby limitation).
      # Disable the propagator by unsetting the flag — it no-ops when disabled.
    end

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def self._install_around_hook
      ::Pinot::Instrumentation.around = method(:_around)
    end
    private_class_method :_install_around_hook

    def self._around(table:, query:)
      unless @enabled
        # Bypass tracing; still time and notify listeners.
        return ::Pinot::Instrumentation.send(:_timed_instrument, table: table, query: query) { yield }
      end

      tracer = ::OpenTelemetry.tracer_provider.tracer(TRACER_NAME, ::Pinot::VERSION)
      attrs  = _span_attributes(table, query)

      tracer.in_span(SPAN_NAME, attributes: attrs, kind: :client) do |span|
        start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        begin
          result = yield
          duration_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000
          ::Pinot::Instrumentation.notify(
            table: table, query: query, duration_ms: duration_ms, success: true, error: nil
          )
          result
        rescue StandardError => e
          duration_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000
          span.record_exception(e)
          span.status = ::OpenTelemetry::Trace::Status.error(e.message)
          ::Pinot::Instrumentation.notify(
            table: table, query: query, duration_ms: duration_ms, success: false, error: e
          )
          raise
        end
      end
    end
    private_class_method :_around

    def self._span_attributes(table, query)
      attrs = {
        "db.system" => DB_SYSTEM,
        "db.statement" => query,
        "db.name" => table.to_s
      }
      op = query.to_s.lstrip.split(/\s+/, 2).first&.upcase
      attrs["db.operation"] = op if op && !op.empty?
      attrs
    end
    private_class_method :_span_attributes

    # Patch JsonHttpTransport to inject W3C trace-context headers into every
    # outbound HTTP request. Applied once via Module#prepend.
    def self._patch_transport
      return if ::Pinot::JsonHttpTransport.ancestors.include?(TraceContextInjector)

      ::Pinot::JsonHttpTransport.prepend(TraceContextInjector)
    end
    private_class_method :_patch_transport

    # Prepended into JsonHttpTransport. Injects traceparent/tracestate into
    # request headers when the bridge is enabled and a current span exists.
    module TraceContextInjector
      def execute(broker_address, request, extra_request_headers: {})
        otel = ::Pinot::OpenTelemetry
        return super unless otel.installed? && otel.enabled?

        carrier = {}
        ::OpenTelemetry.propagation.inject(carrier)
        super(broker_address, request, extra_request_headers: extra_request_headers.merge(carrier))
      end
    end
  end
end
