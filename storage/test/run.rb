$:.push('gen-rb')

require 'Storage'

require 'test/ts_basic_storage'

require 'test/unit'
require 'test/unit/ui/console/testrunner'

Dir.glob("engines/*/harness.rb").each do |engine|
  require engine
  engine_name = engine[/engines\/(.*)\/harness.rb/,1]
  $ENGINE = SCADS::Storage.const_get(engine_name.capitalize)::TestHarness

  puts "==RUNNING TESTS FOR #{engine_name} ENGINE=="
  Test::Unit::UI::Console::TestRunner.run(TS_BasicStorage)
end
