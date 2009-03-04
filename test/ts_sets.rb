require 'test/unit'

class TS_Sets < Test::Unit::TestCase
  def setup
    @server = StorageServer.new
  end
  
  def teardown
    @server.stop
  end
  
  def test_range
  end
  
  def test_empty_range
    #test both sides
  end
  
  def test_trivial_range # same start and end
  end
  
  def test_range_limit
  end
  
  def test_user_function
  end
  
  def test_range_or_func
  end
end