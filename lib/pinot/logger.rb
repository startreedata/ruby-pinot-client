require "logger"

module Pinot
  module Logging
    def self.logger
      @logger ||= begin
        l = Logger.new($stdout)
        l.level = Logger::WARN
        l
      end
    end

    def self.logger=(logger)
      @logger = logger
    end
  end
end
