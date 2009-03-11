require 'test/unit'

class TS_Responsibility < Test::Unit::TestCase
  include SCADS::Storage
  
  def setup
    @server = $ENGINE.new
    @evensfunc = UserFunction.new(:lang=> Language::LANG_RUBY, :func=>"Proc.new {|val| val.to_i%2==0}")
    @favlist = ["1","18","45","32","22"]
  end

  def teardown
    @server.stop
  end

  def test_responsibility_limit_nil 
    policy = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"10",:offset => 1,:limit => 10)
      )
      
    assert_raise(InvalidSetDescription,"start and end limits don't make sense for responsibility policies") do
      @server.set_responsibility_policy("nillimit", policy)
    end
    
  end

  def test_get_responsibility      
      # try a range responsibility
      policy = RecordSet.new(
        :type =>RecordSetType::RST_RANGE,
        :range => RangeSet.new(:start_key=>"01",:end_key=>"10",:offset => nil,:limit => nil)
        )
      @server.set_responsibility_policy("getresp1", policy)
        
      server_rs = @server.get_responsibility_policy("getresp1")  
      assert_equal(RecordSetType::RST_RANGE, server_rs.type)
      assert_equal("01",server_rs.range.start_key)
      assert_equal("10",server_rs.range.end_key)
      assert_nil(server_rs.range.offset)
      assert_nil(server_rs.range.limit)
      
      # try a function responsibility
      policy = RecordSet.new(
        :type =>RecordSetType::RST_KEY_FUNC,
        :func =>@evensfunc
        )
      @server.set_responsibility_policy("getresp2", policy)
        
      server_rp = @server.get_responsibility_policy("getresp2")  
      assert_equal(policy, server_rp)
  end

  def test_range 
    policy = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"10",:offset => nil,:limit => nil)
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
    # TODO
  end

  def test_set 
    desired = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"05",:end_key=>"09",:offset => nil,:limit => nil)
      )
    
    # desired records are a subset of the policy
    policy = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"10",:offset => nil,:limit => nil)
      )
    @server.set_responsibility_policy("listresp", policy) 
    (5..8).each do |i| # set some values
      @server.put("listresp", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    record_list = @server.get_set("listresp",desired) # request a set of records
    
    assert_equal((5..8).map{|i| Record.new(:key => "0#{i}", :value => "val0#{i}")}, record_list)

    # now check if part of desired records are out of this server's responsibility
    # weird semantics, but since server is not responsibility for ALL the keys...
    policy = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"08",:offset => nil,:limit => nil)
      )
    @server.set_responsibility_policy("listresp2", policy)
    (5..8).each do |i| # set some values
      @server.put("listresp2", Record.new(:key => "0#{i}", :value => "val0#{i}"))
    end
    
    assert_raise(NotResponsible, "asked for set that's partly out of server's responsibility") do
      record_list = @server.get_set("listresp2",desired)
    end
  end

  def test_user_function
    policy = RecordSet.new(
      :type =>RecordSetType::RST_KEY_FUNC,
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
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"06",:end_key=>"10",:offset => nil,:limit => nil)
      )
    @server.set_responsibility_policy("rightresp1", policy)
    
    policy2 = RecordSet.new(
      :type =>RecordSetType::RST_RANGE,
      :range => RangeSet.new(:start_key=>"01",:end_key=>"05",:offset => nil,:limit => nil)
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