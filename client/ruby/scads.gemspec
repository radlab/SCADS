Gem::Specification.new do |s|
  s.name = "scads"
  s.version = "0.0.6"
  s.date = "2009-05-23"
  s.summary = "SCADS modules"
  s.require_paths = ["gen-rb", "."]
  s.files = Dir['scads.rb'] + Dir['gen-rb/*.rb']
  s.add_dependency('thrift', '>= 0.0.1')
end