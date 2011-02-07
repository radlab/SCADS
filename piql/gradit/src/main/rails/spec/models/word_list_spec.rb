require 'spec_helper'

describe WordList do
  before(:all) do
    @wl = WordList.createNew("wordlist")
  end

  it "should create a new instance given valid attributes" do
    @wl.should_not == nil
  end
  
  it "should be able to find a WordList by PK" do
    WordList.find("wordlist").should_not == nil
  end
  
  it "should be able to find words belonging to it" do 
    Word.createNew(2001, "wordlistspec1", "word list spec one", "wordlist")
    Word.createNew(2002, "wordlistspec2", "word list spec two", "wordlist")
    Word.createNew(2003, "wordlistspec3", "word list spec three", "wordlist")
    
    @wl.words.size.should == 2
  end
end
