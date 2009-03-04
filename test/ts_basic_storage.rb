require 'test/unit'

class TS_BasicStorage < Test::Unit::TestCase
  def setup
    @server = StorageServer.new
  end
  
  def teardown
    @server.stop
  end
  
  def test_get_put
    (0..10).each do |i|
      @server.put("getput", Record.new(:key => "key#{i}", :value => "value#{i}"))
    end
    
    (0..10).each do |i|
      assert_equal("value#{i}", @server.get("getput", "key#{i}"))
    end
  end
  
  def test_update_value
  end
  
  def test_set_nil
  end
end





