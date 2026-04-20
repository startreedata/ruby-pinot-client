require "pinot"
require "pinot/active_support_notifications"

module Pinot
  # Rails integration via Railtie — zero configuration required.
  #
  # Automatically activated when the gem is required inside a Rails app.
  # Just add `gem "pinot-client"` to your Gemfile.
  #
  # == What it does
  #
  # 1. **ActiveSupport::Notifications bridge** — every Pinot query fires a
  #    "sql.pinot" event on the AS::N bus, picked up by Rails log subscribers,
  #    Skylight, Scout APM, etc.
  #
  # 2. **OpenTelemetry bridge** — when the opentelemetry-api gem is present,
  #    creates "pinot.query" spans and injects W3C trace-context headers into
  #    every outbound broker request.
  #
  # 3. **X-Request-Id propagation** — inserts Pinot::RequestIdMiddleware into
  #    the Rack stack. The current request's X-Request-Id is forwarded as an
  #    HTTP header on every broker call so Pinot broker logs can be correlated
  #    with application request logs.
  #
  # == Opting out of individual features
  #
  #   # config/initializers/pinot.rb
  #   Rails.application.config.pinot.notifications  = false
  #   Rails.application.config.pinot.open_telemetry = false
  #   Rails.application.config.pinot.request_id     = false
  #
  # == Manual setup (non-Rails / opt-out of Railtie entirely)
  #
  #   require "pinot/active_support_notifications"
  #   Pinot::ActiveSupportNotifications.install!
  #
  #   require "pinot/open_telemetry"
  #   Pinot::OpenTelemetry.install!
  #
  #   # In your Rack middleware stack:
  #   use Pinot::RequestIdMiddleware
  class Railtie < ::Rails::Railtie
    initializer "pinot.install_notifications" do |app|
      opts = app.config.pinot
      next if opts[:notifications] == false

      require "pinot/active_support_notifications"
      ActiveSupportNotifications.install!
    end

    initializer "pinot.install_open_telemetry" do |app|
      opts = app.config.pinot
      next if opts[:open_telemetry] == false

      begin
        require "opentelemetry"
        require "pinot/open_telemetry"
        OpenTelemetry.install!
      rescue LoadError
        # opentelemetry-api gem not present — skip silently
      end
    end

    initializer "pinot.request_id_propagation" do |app|
      opts = app.config.pinot
      next if opts[:request_id] == false

      app.config.middleware.use(RequestIdMiddleware)
      Connection.prepend(RequestIdInjector)
    end
  end

  # Rack middleware that captures X-Request-Id from the inbound HTTP request
  # and stores it in a thread-local for the duration of the request.
  #
  # Inserted automatically by Pinot::Railtie. For non-Rails Rack apps:
  #
  #   use Pinot::RequestIdMiddleware
  class RequestIdMiddleware
    RACK_HEADER = "HTTP_X_REQUEST_ID".freeze

    def initialize(app)
      @app = app
    end

    def call(env)
      Thread.current[:pinot_request_id] = env[RACK_HEADER]
      @app.call(env)
    ensure
      Thread.current[:pinot_request_id] = nil
    end
  end

  # Prepended into Connection when the Railtie is active.
  # Automatically merges the current request's X-Request-Id into every
  # outbound Pinot query as an HTTP header, without requiring callers to
  # pass it explicitly.
  module RequestIdInjector
    def execute_sql(table, query, query_timeout_ms: nil, headers: {})
      rid = Thread.current[:pinot_request_id]
      merged = rid && !headers.key?("X-Request-Id") ? headers.merge("X-Request-Id" => rid) : headers
      super(table, query, query_timeout_ms: query_timeout_ms, headers: merged)
    end
  end
end
