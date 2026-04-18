require_relative "lib/pinot/version"

Gem::Specification.new do |spec|
  spec.name = "pinot"
  spec.version = Pinot::VERSION
  spec.authors = ["Xiang Fu"]
  spec.summary = "Apache Pinot Ruby client"
  spec.description = "A Ruby client for Apache Pinot, mirroring the Go client API"
  spec.homepage = "https://github.com/startreedata/ruby-pinot-client"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 2.6"

  spec.files = Dir["lib/**/*", "LICENSE", "README.md"]
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "webmock", "~> 3.18"
  spec.add_development_dependency "rake", "~> 13.0"
end
