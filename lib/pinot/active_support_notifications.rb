require "pinot"

module Pinot
  # One-line ActiveSupport::Notifications bridge for Rails apps.
  #
  #   # config/initializers/pinot.rb
  #   require "pinot/active_support_notifications"
  #   Pinot::ActiveSupportNotifications.install!
  #
  # Every query executed via Connection#execute_sql will then publish a
  # "sql.pinot" event on the ActiveSupport::Notifications bus.
  #
  # Payload keys:
  #   :sql        — the query string
  #   :name       — the table name (may be empty string for table-less queries)
  #   :duration   — execution time in milliseconds (Float)
  #   :success    — boolean
  #   :exception        — [ExceptionClassName, message] or nil  (AS::N convention)
  #   :exception_object — the raw exception or nil              (AS::N convention)
  module ActiveSupportNotifications
    EVENT_NAME = "sql.pinot"

    def self.install!
      return if installed?

      Pinot::Instrumentation.on_query = method(:notify)
      @installed = true
    end

    def self.installed?
      @installed || false
    end

    def self.uninstall!
      Pinot::Instrumentation.on_query = nil
      @installed = false
    end

    def self.notify(event)
      payload = {
        sql:      event[:query],
        name:     event[:table],
        duration: event[:duration_ms],
        success:  event[:success]
      }

      if (err = event[:error])
        payload[:exception]        = [err.class.name, err.message]
        payload[:exception_object] = err
      end

      ::ActiveSupport::Notifications.instrument(EVENT_NAME, payload)
    end
    private_class_method :notify
  end
end
