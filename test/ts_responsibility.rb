require 'test/unit'

class TS_Responsibility < Test::Unit::TestCase
  def setup
    @server = StorageServer.new
    @evensfunc = UserFunction.new(:lang=> Language::RUBY, :func=>"def evens(val) val.to_i%2==0 end")
  end
  
  def teardown
    @server.stop
  end
  
  def test_responsibility_limit_nil 
    policy = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"10",:start_limit => 1,:end_limit => 10)
      )
      
    assert_raise(InvalidSetDescription,"start and end limits don't make sense for responsibility policies") do
      @server.set_responsibility_policy("nillimit", policy)
    end
    
  end
  
  def test_get_responsibility
      
      # try a range responsibility
      policy = RecordSet.new(
        :type =>RecordSetType::RANGE,
        :range => RangeSet.new(:start_key=>"01",:end_key=>"10",:start_limit => nil,:end_limit => nil)
        )
      @server.set_responsibility_policy("getresp1", policy)
        
      server_rs = @server.get_responsibility_policy("getresp1")  
      assert_equal(RecordSetType::RANGE, server_rs.type)
      assert_equal("01",server_rs.range.start_key)
      assert_equal("10",server_rs.range.end_key)
      assert_nil(server_rs.range.start_limit)
      assert_nil(server_rs.range.end_limit)
      
      # try a function responsibility
      policy = RecordSet.new(
        :type =>RecordSetType::KEY_FUNC,
        :func =>@evensfunc
        )
      @server.set_responsibility_policy("getresp2", policy)
        
      server_rs = @server.get_responsibility_policy("getresp2")  
      assert_equal(RecordSetType::KEY_FUNC, server_rs.type)
      assert_equal("def evens(val) val.to_i%2==0 end",server_rs.func.func)

      
  end
  
  def test_range 
    policy = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"10",:start_limit => nil,:end_limit => nil)
      )
      
    @server.set_responsibility_policy("rangeresp", policy)
    
    assert_raise(NotResponsible) do     # left outside
     @server.put("rangeresp", Record.new(:key => "04", :value => "val04"))
    end
    
    assert(@server.put("rangeresp", Record.new(:key => "05", :value => "val05"))) # edgecase
    assert(@server.put("rangeresp", Record.new(:key => "08", :value => "val08"))) # middle
    assert(@server.put("rangeresp", Record.new(:key => "10", :value => "val10"))) # edgecase
    
    assert_raise(NotResponsible) do    # right outside
       @server.put("rangeresp", Record.new(:key => "key11", :value => "value11"))
    end
    
  end
  
  def test_list 
    desired = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"09",:start_limit => nil,:end_limit => nil)
      )
    
    # desired records are a subset of the policy
    policy = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"10",:start_limit => nil,:end_limit => nil)
      )
    @server.set_responsibility_policy("listresp", policy) 
    (5..8).each do |i| # set some values
      @server.put("listresp", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    record_list = @server.get_set("listresp",desired) # request a set of records
    
    assert_equal(4,record_list.size,"# records expected")
  
    # check get back all *existing* values in the desired range
    assert(!list_contains(record_list,Record.new(:key => "04", :value => "val04")),"list has extra value") # not desired
    (5..8).each do |i| # got all desired that exist
      assert(list_contains(record_list,Record.new(:key => "0#{i}", :value => "val0#{i}")),"list missing value")
    end
    assert(!list_contains(record_list,Record.new(:key => "09", :value => "val09")),"list has extra value") # desired, but doesn't exist
    assert(!list_contains(record_list,Record.new(:key => "10", :value => "val10")),"list has extra value") # not desired

    # now check if part of desired records are out of this server's responsibility
    # weird semantics, but since server is not responsibility for ALL the keys...
    policy = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:start_limit => nil,:end_limit => nil)
      )
    @server.set_responsibility_policy("listresp2", policy)
    (5..8).each do |i| # set some values
      @server.put("listresp2", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    assert_raise(NotResponsible, "asked for set that's partly out of server's responsibility") do
      record_list = @server.get_set("listresp2",desired)
    end

  end
  
  
  def list_contains(list,record) # helper function for checking test_list
    list.each do |rec|
      if rec.key.eql?(record.key) and rec.value.eql?(record.value)
        return true
      end
    end
    return false
  end
  
  def test_user_function
    policy = RecordSet.new(
      :type =>RecordSetType::KEY_FUNC,
      :func =>@evensfunc
      )
      
    @server.set_responsibility_policy("funcresp", policy)
  
    assert(@server.put("funcresp", Record.new(:key => "04", :value => "val04"))) # inside    
    assert_raise(NotResponsible) do     # outside
      @server.put("funcresp", Record.new(:key => "01", :value => "val01"))
    end
  
  end
  
  
  def test_right_responsibility
    policy = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"06",:end_key=>"10",:start_limit => nil,:end_limit => nil)
      )
    @server.set_responsibility_policy("rightresp1", policy)
    
    policy2 = RecordSet.new(
      :type =>RecordSetType::RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"05",:start_limit => nil,:end_limit => nil)
      )  
    @server.set_responsibility_policy("rightresp2", policy2)
    
    # can't mix up responsibilities across namespaces 
    assert(@server.put("rightresp1", Record.new(:key => "08", :value => "val08")))
    assert_raise(NotResponsible) do 
     @server.put("rightresp1", Record.new(:key => "04", :value => "val04"))
    end
    
    assert(@server.put("rightresp2", Record.new(:key => "04", :value => "val04")))
    assert_raise(NotResponsible) do 
     @server.put("rightresp2", Record.new(:key => "08", :value => "val08"))
    end    
    
  end

end