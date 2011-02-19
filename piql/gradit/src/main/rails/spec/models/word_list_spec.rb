require 'spec_helper'

describe WordList do
  before(:all) do
    @wl = WordList.createNew("wl_spec")
  end

  it "should create a new instance given valid attributes" do
    @wl.should_not == nil
  end
  
  it "should be able to find a WordList by PK" do
    WordList.find("wl_spec").should_not == nil
  end
  
  it "should be able to find words belonging to it" do 
    Word.createNew(1001, "wordlistspec1", "word list spec one", "wl_spec")
    Word.createNew(1002, "wordlistspec2", "word list spec two", "wl_spec")
    Word.createNew(1003, "wordlistspec3", "word list spec three", "wl_spec")
    Word.createNew(1004, "wordlistspec3", "word list spec three", "wl_spec2")
    
    @wl.words.size.should == 3
  end
end
