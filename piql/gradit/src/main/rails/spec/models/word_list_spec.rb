require 'spec_helper'

describe WordList do
  before(:each) do
  end

  it "should create a new instance given valid attributes" do
    WordList.createNew("wordlist").should_not == nil
  end
  
  it "should be able to find a WordList by PK" do
  end
  
  it "should be able to find words belonging to it" do 
  end
end
