require 'spec_helper'

describe Word do
  before(:each) do
  end

  it "should create a new instance given valid attributes" do
    Word.createNew(1, "vex", "definition", "wordlist").should_not == nil
  end
  
  it "should be able to find a Word by PK" do
  end
  
  it "should be able to find a Word by word" do 
  end
  
  it "should be able to find contexts belonging to it" do
  end
  
  it "should be able to generate multiple choices from its wordlist" do
  end
end
