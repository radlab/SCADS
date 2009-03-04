require 'test/unit'

class TS_Sets < Test::Unit::TestCase
  def setup
    @server = StorageServer.new
    @evensfunc = UserFunction.new(:lang=> Language::RUBY, :func=>"def evens(val) val.to_i%2==0 end")
  end
  
  def teardown
    @server.stop
  end
  
  def test_range
    (1..8).each do |i| # set some values
      @server.put("rangeset", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"09",:start_limit => nil,:end_limit => nil)
      )
    record_list = @server.get_set("rangeset",desired)
    
    assert_equal(4,record_list.size,"# records expected")
  
    # check get back all *existing* values in the desired range
    assert(!list_contains(record_list,Record.new(:key => "04", :value => "val04")),"list has extra value") # not desired
    (5..8).each do |i| # got all desired that exist
      assert(list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"list missing value")
    end
    assert(!list_contains(record_list,Record.new(:key => "09", :value => "val09")),"list has extra value") # desired, but doesn't exist
    assert(!list_contains(record_list,Record.new(:key => "10", :value => "val10")),"list has extra value") # not desired

  end
  
  def test_empty_range
    (1..8).each do |i| # set some values
      @server.put("emptyrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # these values don't exist
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"09",:end_key=>"11",:start_limit => nil,:end_limit => nil)
      )
    record_list = @server.get_set("emptyrange",desired)
    assert_equal(0,record_list.size,"# records expected")
    
    # test both sides
    # ?
    
  end
  
  def test_trivial_range 
    (1..8).each do |i| # set some values
      @server.put("trivialrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # same start and end... one value that exists
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"07",:end_key=>"07",:start_limit => nil,:end_limit => nil)
      )
    record_list = @server.get_set("trivialrange",desired)
    assert_equal(1,record_list.size,"# records expected")
    assert(list_contains(record_list,Record.new(:key => "08", :value => "val08")),"list missing value")
    
  end
  
  def test_range_limit
    (0..9).each do |i| # set some values
      @server.put("rangelimit", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # should return first four values from values matching the range
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:start_limit => 0,:end_limit => 3)
      )
    record_list = @server.get_set("rangelimit",desired)
    (1..4).each do |i| # got all desired in limit
      assert(list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"list missing value")
    end
    (5..8).each do |i| # didn't get keys not in limit
      assert(!list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"extra value")
    end
    
   # should return last two values from values matching the range
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:start_limit => 6,:end_limit => 10)
      )
    record_list = @server.get_set("rangelimit",desired)
    (7..8).each do |i| # got all desired in limit
      assert(list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"list missing value")
    end
    (1..6).each do |i| # didn't get keys not in limit
      assert(!list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"extra value")
    end
    # make sure didn't extend beyond range definition
    assert(!list_contains(record_list,Record.new(:key => "09", :value => "val09")),"extra value")
  
  end
  
  def test_user_function
    (1..8).each do |i| # set some values
      @server.put("userrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # ask for only the evens
    desired = RecordSet.new(
      :type =>RecordSetType::KEY_FUNC,
      :func => @evensfunc
      )
    record_list = @server.get_set("userrange",desired)
    assert_equal(4,record_list.size,"# records expected")
    
    (1..8).each do |i|
      if i%2==0
        assert(list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"list missing value")
      else
        assert(!list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"extra value")
      end
    end
  end
  
  def test_invalid_description
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"1",:end_key=>"0",:start_limit => nil,:end_limit => nil)
      )
      
    assert_raise(InvalidSetDescription,"end key is less than start key") do
      record_list = @server.get_set("invalidset",desired)
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>nil,:end_key=>"10",:start_limit => nil,:end_limit => nil)
      )
      
    assert_raise(InvalidSetDescription,"start key is nil") do
      record_list = @server.get_set("invalidset",desired)
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"1",:end_key=>nil,:start_limit => nil,:end_limit => nil)
      )
      
    assert_raise(InvalidSetDescription,"end key is nil") do
      record_list = @server.get_set("invalidset",desired)
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"09",:start_limit => 5,:end_limit => 1)
      )
      
    assert_raise(InvalidSetDescription,"start limit is more than end limit") do
      record_list = @server.get_set("invalidset",desired)
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"09",:start_limit => nil,:end_limit => nil),
      :func=>@evensfunc
      )
      
    assert_raise(InvalidSetDescription,"can't specify range and func in RecordSet") do
      record_list = @server.get_set("invalidset",desired)
    end
    
  end
  
  def list_contains(list,record) # helper function for checking list contents
    list.each do |rec|
      if rec.key.eql?(record.key) and rec.value.eql?(record.value)
        return true
      end
    end
    return false
  end
  
end