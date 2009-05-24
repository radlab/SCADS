Gem::Specification.new do |s|
  s.name = "scads"
  s.version = "0.0.5"
  s.date = "2009-05-23"
  s.summary = "SCADS modules"
  s.require_paths = ["lib", "storage/gen-rb","storage/engines/simple"]
  s.files = Dir['lib/*.rb'] + Dir['storage/gen-rb/*.rb'] + Dir['storage/engines/simple/*.rb'] + Dir['storage/engines/simple/bin/*']
  s.bindir = "storage/engines/simple/bin"
  s.executables = ["start_scads.rb"] # only one, so this will be default
  s.add_dependency('thrift', '>= 0.0.1')
end