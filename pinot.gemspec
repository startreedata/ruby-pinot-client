require_relative "lib/pinot/version"

Gem::Specification.new do |spec|
  spec.name = "pinot-client"
  spec.version = Pinot::VERSION
  spec.authors = ["Xiang Fu"]
  spec.summary = "Apache Pinot Ruby client"
  spec.description = "A Ruby client for Apache Pinot, mirroring the Go client API"
  spec.homepage = "https://github.com/startreedata/ruby-pinot-client"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 2.6"

  spec.metadata = {
    "homepage_uri"    => spec.homepage,
    "source_code_uri" => spec.homepage,
    "changelog_uri"   => "https://github.com/startreedata/ruby-pinot-client/blob/main/CHANGELOG.md",
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "github_repo"     => "ssh://git@github.com/startreedata/ruby-pinot-client"
  }

  spec.files = Dir["lib/**/*", "LICENSE", "README.md"]
  spec.require_paths = ["lib"]

  spec.add_dependency "logger"

  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "webmock", "~> 3.18"
  spec.add_development_dependency "rake", "~> 13.0"
  # grpc is optional — install manually to use GrpcTransport
  # spec.add_development_dependency "grpc", "~> 1.65"
end
