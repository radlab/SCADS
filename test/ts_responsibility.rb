class TS_Responsibility < Test::Unit::TestCase
  def setup
    @server = StorageServer.new
  end
  
  def teardown
    @server.stop
  end
  
  def test_range
    #outside, edgecase, middle, edgecase, outside
  end
  
  def test_list
  end
  
  def test_user_function
  end
  
  def test_get_responsibility
  end
end