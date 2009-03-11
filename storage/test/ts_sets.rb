require 'test/unit'

class TS_Sets < Test::Unit::TestCase
  include SCADS::Storage
  
  def setup
    @server = $ENGINE.new
    @evensfunc = UserFunction.new(:lang=> Language::LANG_RUBY, :func=>"Proc.new {|val| val.to_i%2==0}")
  end
  
  def teardown
    @server.stop
  end
  
  def test_range
    (1..8).each do |i| # set some values
      @server.put("rangeset", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"09",:offset => nil,:limit => nil)
      )
    record_list = @server.get_set("rangeset",desired)
    
    assert_equal((5..8).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)
  end

  def test_empty_range
    (1..8).each do |i| # set some values
      @server.put("emptyrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # these values don't exist
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"09",:end_key=>"11",:offset => nil,:limit => nil)
      )
    record_list = @server.get_set("emptyrange",desired)
    assert_equal([], record_list)
    
    # test both sides
    # ?
  end

  def test_trivial_range 
    (1..8).each do |i| # set some values
      @server.put("trivialrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # same start and end... one value that exists
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"07",:end_key=>"07",:offset => nil,:limit => nil)
      )
    record_list = @server.get_set("trivialrange",desired)
    assert_equal([Record.new(:key => "07", :value => "val07")], record_list)  
  end

  def test_range_limit
    (0..9).each do |i| # set some values
      @server.put("rangelimit", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # should return first four values from values matching the range
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:offset => 0,:limit => 3)
      )
    record_list = @server.get_set("rangelimit",desired)
    assert_equal((1..4).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)
    
   # should return last two values from values matching the range
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:offset => 6,:limit => 10)
      )
    record_list = @server.get_set("rangelimit",desired)
    assert_equal((7..8).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)
  end

  def test_user_function
    (1..8).each do |i| # set some values
      @server.put("userrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # ask for only the evens
    desired = RecordSet.new(
      :type =>RecordSetType::RST_KEY_FUNC,
      :func => @evensfunc
      )
    record_list = @server.get_set("userrange",desired)
    assert_equal([2,4,6,8].map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)
  end

  def test_nil_range_ends
    (1..8).each do |i| # set some values
      @server.put("nilrange", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>nil,:end_key=>"05",:offset => nil,:limit => nil)
      )
    record_list = @server.get_set("nilrange",desired) # get everything up to 05
    assert_equal((1..5).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)  
    
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"02",:end_key=>nil,:offset => nil,:limit => nil)
      )
    record_list = @server.get_set("nilrange",desired) # get from 02 to end
    assert_equal((2..8).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)
  end

  def test_invalid_description
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"1",:end_key=>"0",:offset => nil,:limit => nil)
      )
      
    assert_raise(InvalidSetDescription,"end key is less than start key") do
      record_list = @server.get_set("invalidset",desired)
    end
    
    
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"09",:offset => 5,:limit => 1)
      )
      
    assert_raise(InvalidSetDescription,"start limit is more than end limit") do
      record_list = @server.get_set("invalidset",desired)
    end
    
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"09",:offset => nil,:limit => nil),
      :func=>@evensfunc
      )
      
    assert_raise(InvalidSetDescription,"can't specify range and func in RecordSet") do
      record_list = @server.get_set("invalidset",desired)
    end    
  end

end