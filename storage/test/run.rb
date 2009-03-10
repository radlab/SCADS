$:.push('gen-rb')

require 'rubygems'
require 'activesupport'
require 'Storage'

require 'test/unit'
require 'test/unit/ui/console/testrunner'

suite = Test::Unit::TestSuite.new

Dir.glob("test/ts_*.rb").each do |ts|
  require ts
  suite << Object.const_get("TS_#{ts[/ts_(\S+).rb/,1].camelize}").suite
end



Dir.glob("engines/*/harness.rb").each do |engine|
  require engine
  engine_name = engine[/engines\/(.*)\/harness.rb/,1]
  $ENGINE = SCADS::Storage.const_get(engine_name.capitalize)::TestHarness

  puts "==RUNNING TESTS FOR #{engine_name} ENGINE=="
  Test::Unit::UI::Console::TestRunner.run(suite)
end
