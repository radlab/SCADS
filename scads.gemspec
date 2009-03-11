Gem::Specification.new do |s|
  s.name = "scads"
  s.version = "0.0.1"
  s.date = "2009-03-10"
  s.summary = "SCADS modules"
  s.require_paths = ["lib", "storage/gen-rb","storage/engines/simple"]
  s.files = Dir['lib/*.rb'] + Dir['storage/gen-rb/*.rb'] + Dir['storage/engines/simple/*.rb'] + Dir['storage/engines/simple/bin/*']
  s.bindir = "storage/engines/simple/bin"
  s.executables = ["start_server.rb"] # only one, so this will be default
  s.add_dependency('thrift', '>= 0.0.1')
end