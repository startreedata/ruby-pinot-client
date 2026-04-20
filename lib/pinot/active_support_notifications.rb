require "pinot"

module Pinot
  # Opt-in ActiveSupport::Notifications bridge for Rails applications.
  #
  # == Setup
  #
  # Add one line to an initializer (e.g. config/initializers/pinot.rb):
  #
  #   require "pinot/active_support_notifications"
  #   Pinot::ActiveSupportNotifications.install!
  #
  # That's it. Every query executed via Connection#execute_sql (including those
  # from execute_sql_with_params, execute_many, and PreparedStatement) will
  # publish a "sql.pinot" event on the ActiveSupport::Notifications bus.
  #
  # == Subscribing
  #
  #   ActiveSupport::Notifications.subscribe("sql.pinot") do |name, start, finish, id, payload|
  #     Rails.logger.debug "[Pinot] #{payload[:name]} — #{payload[:sql]} (#{payload[:duration].round(1)} ms)"
  #   end
  #
  # == Payload keys
  #
  #   :sql              — the SQL query string
  #   :name             — the Pinot table name (empty string for table-less queries)
  #   :duration         — execution time in milliseconds (Float)
  #   :success          — true on success, false when an exception was raised
  #   :exception        — [ExceptionClassName, message] on error, absent on success
  #                       (follows the ActiveSupport::Notifications convention)
  #   :exception_object — the raw exception on error, absent on success
  #                       (follows the ActiveSupport::Notifications convention)
  #
  # == Lifecycle
  #
  # The bridge is installed idempotently:
  #
  #   Pinot::ActiveSupportNotifications.install!   # register
  #   Pinot::ActiveSupportNotifications.installed? # => true
  #   Pinot::ActiveSupportNotifications.uninstall! # deregister (e.g. in tests)
  #
  # Note: this gem does NOT depend on activesupport. The bridge requires
  # ActiveSupport::Notifications to already be defined at install! time (which
  # is always the case in a Rails process).
  module ActiveSupportNotifications
    EVENT_NAME = "sql.pinot".freeze

    def self.install!
      return if installed?

      @listener  = Pinot::Instrumentation.subscribe(method(:notify))
      @installed = true
    end

    def self.installed?
      @installed || false
    end

    def self.uninstall!
      Pinot::Instrumentation.unsubscribe(@listener) if @listener
      @listener  = nil
      @installed = false
    end

    def self.notify(event)
      payload = {
        sql: event[:query],
        name: event[:table],
        duration: event[:duration_ms],
        success: event[:success]
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
