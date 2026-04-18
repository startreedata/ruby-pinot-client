require "webmock"
# Allow real HTTP connections for integration tests
WebMock.allow_net_connect!

require "pinot"

BROKER_HOST = ENV.fetch("BROKER_HOST", "127.0.0.1")
BROKER_PORT = ENV.fetch("BROKER_PORT", "8000")

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
  # Ensure net connect stays enabled even if spec_helper was loaded first
  config.before(:suite) do
    WebMock.allow_net_connect!
  end
end

def pinot_client(use_multistage: false)
  conn = Pinot.from_broker_list(["#{BROKER_HOST}:#{BROKER_PORT}"])
  conn.use_multistage_engine = use_multistage
  conn
end
