require "simplecov"
require "simplecov-lcov"

SimpleCov::Formatter::LcovFormatter.config do |c|
  c.report_with_single_file = true
  c.output_directory = "coverage"
  c.lcov_file_name = "lcov.info"
end

SimpleCov.start do
  formatter SimpleCov::Formatter::MultiFormatter.new([
    SimpleCov::Formatter::HTMLFormatter,
    SimpleCov::Formatter::LcovFormatter
  ])
  add_filter "/spec/"
  add_filter "/vendor/"
  add_filter "/proto/"
  track_files "lib/**/*.rb"
  # Only enforce minimum coverage during unit test runs (not integration tests,
  # which intentionally exercise a narrow slice of code).
  minimum_coverage ENV.fetch("COVERAGE_MIN", 0).to_i
end

require "pinot"
require "webmock/rspec"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  WebMock.disable_net_connect!
end
