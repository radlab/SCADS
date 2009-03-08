$:.push('gen-rb')

require 'Storage'

require 'test/ts_basic_storage'
require 'test/ts_sets'
require 'test/ts_syncing'
require 'test/ts_responsibility'



Dir.glob("engines/*/harness.rb").each do |engine|
  require engine
  engine_name = engine[/engines\/(.*)\/harness.rb/,1]
  $ENGINE = SCADS::Storage.const_get(engine_name.capitalize)::TestHarness
end