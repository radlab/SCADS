require 'test/unit'

class TS_Syncing < Test::Unit::TestCase
  def setup
    @server1 = StorageServer.new
#    @server2 = StorageServer.new
  end
  
  def teardown
    @server1.stop
#    @server2.stop
  end
  
  def test_copy
  end
  
  def test_remove
  end
  
  def test_sync_greater
  end
  
  def test_sync_user_function
  end
end