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
  minimum_coverage 80
  track_files "lib/**/*.rb"
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
