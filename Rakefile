require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec) do |t|
  t.exclude_pattern = "spec/integration/**/*_spec.rb"
end

RSpec::Core::RakeTask.new(:integration) do |t|
  t.pattern = "spec/integration/**/*_spec.rb"
  t.rspec_opts = "--require spec/integration/integration_helper"
end

task default: :spec
