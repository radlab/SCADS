require 'spec_helper'

describe Word do
  before(:all) do
    @w = Word.createNew(1, "vex", "definition", "wordlist")
  end

  it "should create a new instance given valid attributes" do
    @w.should_not == nil
  end
  
  it "should be able to find a Word by PK" do
    Word.find(1).should_not == nil
  end
  
  it "should be able to find a Word by word" do 
    Word.find_word_by_word("vex").should_not == nil
  end
  
  it "should be able to find contexts belonging to it" do
    WordContext.createNew(1, "Heart of Wuthering Eyre", 3812, "vex vex vex vex vex.  More vex.")
    WordContext.createNew(1, "Heart of Wuthering Eyre", 3813, "This appears once. vex.")
    WordContext.createNew(1, "BookName!", 0, "vex")
    
    @w.contexts.size.should > 0
  end
  
  it "should be able to generate multiple choices from its wordlist" do
    WordList.createNew("word_spec_wl")
    
    nw = Word.createNew(1001, "wordspec1", "word spec one", "word_spec_wl")
    Word.createNew(1002, "wordspec2", "word spec two", "word_spec_wl")
    Word.createNew(1003, "wordspec3", "word spec three", "word_spec_wl")
    Word.createNew(1004, "wordspec4", "word spec four", "word_spec_wl")
    Word.createNew(1005, "wordspec5", "word spec five", "word_spec_wl")
    
    nw.choices.size.should == 3
  end
end
