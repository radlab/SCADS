require 'test/unit'

class TS_Syncing < Test::Unit::TestCase  
  include SCADS::Storage
  
  def setup
    @server1 = $ENGINE.new
    @server2 = $ENGINE.new
    
    @policy_greater = ConflictPolicy.new(:type=> ConflictPolicyType::CPT_GREATER)
    @policy_merge= ConflictPolicy.new(:type=> ConflictPolicyType::CPT_FUNC, 
    :func=>UserFunction.new(:lang=> Language::LANG_RUBY, :func=>"Proc.new {|val1,val2| [eval(val1),eval(val2)].flatten.uniq.sort.inspect}"))
  end
  
  def teardown
    @server1.stop
    @server2.stop
  end
  
  def test_copy
    (1..9).each do |i| # set some values
      @server1.put("copyset", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # copy some of the values to another server
    to_copy = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"09",:offset => nil,:limit => nil)
      )
    @server1.copy_set("copyset", to_copy,@server2.sync_host)
    
    # try to get values from both servers
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"09",:offset => nil,:limit => nil)
      )
    record_list1 = @server1.get_set("copyset",desired) # should sucessfully get 01-09
    assert_equal((1..9).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list1)
  
    record_list2 = @server2.get_set("copyset",desired) # should get the copied values, 05-09
    assert_equal((5..9).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list2)
  end
  
  def test_remove
    (1..9).each do |i| # set some values
      @server1.put("removeset", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # remove some of the values
    to_remove = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"09",:offset => nil,:limit => nil)
      )
    @server1.remove_set("removeset", to_remove)
    
    # try to get all the values, should only get part of list that hasn't been removed
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"09",:offset => nil,:limit => nil)
      )
    record_list = @server1.get_set("removeset",desired)
    assert_equal((1..4).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)
  end
  
  def test_sync_greater
    (1..4).each do |i| # set some values for one server
      @server1.put("syncgreater", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    (5..8).each do |i| # set some values for one server
      @server1.put("syncgreater", Record.new(:key => "0#{i}", :value => "val0#{i+1}"))
    end
    (1..4).each do |i| # set some values for another server (same keys)
      @server2.put("syncgreater", Record.new(:key => "0#{i}", :value => "val0#{i+1}"))
    end
    (5..8).each do |i| # set some values for another server (same keys)
      @server2.put("syncgreater", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    # sync some of the values from server1 and server2
    to_sync = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"07",:end_key=>"08",:offset => nil,:limit => nil)
      )
    @server1.sync_set("syncgreater", to_sync,@server2.sync_host, @policy_greater)
    
    # try to get values from both servers
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:offset => nil,:limit => nil)
      )
    record_list1 = @server1.get_set("syncgreater",desired) # vals 01,02,03,04,06,07,08,09
    assert_equal(
      (1..4).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}.concat((5..8).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i+1}")}), 
      record_list1)
    
    record_list2 = @server2.get_set("syncgreater",desired) # vals 02,03,04,05,05,06,08,09
    assert_equal(
      (1..4).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i+1}")}.concat((5..6).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}).concat((7..8).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i+1}")}),
       record_list2)
    
  end
  
  def test_sync_user_function
    (1..8).each do |i| # set some values for one server
      @server1.put("syncfunc", Record.new(:key => "0#{i}", :value => ["0#{i}"].inspect))
    end
    (1..4).each do |i| # set some values for other server
      @server2.put("syncfunc", Record.new(:key => "0#{i}", :value => ["0#{i*2}"].inspect))
    end
    (5..8).each do |i| # set some values for other server
      @server2.put("syncfunc", Record.new(:key => "0#{i}", :value => ["#{i*2}"].inspect))
    end
    
    # sync some of the values from server1 and server2
    to_sync = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"08",:offset => nil,:limit => nil)
      )
    @server1.sync_set("syncfunc", to_sync,@server2.sync_host, @policy_merge)
  
    # try to get values from both servers
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:offset => nil,:limit => nil)
      )
    record_list1 = @server1.get_set("syncfunc",desired) # vals [01],[02],[03],[04],[05,10],[06,12],[07,14],[08,16]
    assert_equal(
      (1..4).map{|i| Record.new(:key => "0#{i}", :value => ["0#{i}"].inspect)}.concat((5..8).map{|i| Record.new(:key => "0#{i}", :value => ["0#{i}","#{i*2}"].inspect)}), 
      record_list1)
  
    record_list2 = @server2.get_set("syncfunc",desired) # vals [02],[04],[06],[08],[05,10],[06,12],[07,14],[08,16]
    assert_equal(
      (1..4).map{|i| Record.new(:key => "0#{i}", :value => ["0#{i*2}"].inspect)}.concat((5..8).map{|i| Record.new(:key => "0#{i}", :value => ["0#{i}","#{i*2}"].inspect)}), 
      record_list2)
  end
  
end
