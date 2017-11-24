lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-rds-mysql-log"
  spec.version       = "0.1.9"
  spec.authors       = ["shinsaka", "Mitsuhiro Tanda"]
  spec.email         = ["shinx1265@gmail.com", "mitsuhiro.tanda@gmail.com"]
  spec.summary       = "Amazon RDS for MySQL log input plugin"
  spec.description   = "fluentd plugin for Amazon RDS for MySQL log input"
  spec.homepage      = "https://github.com/mtanda/fluent-plugin-rds-mysql-log"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "fluentd", "~> 0"
  spec.add_dependency "aws-sdk", "~> 2"
  spec.add_dependency "myslog", "~> 0.0"

  spec.add_development_dependency "bundler", "~> 1.7"
end
