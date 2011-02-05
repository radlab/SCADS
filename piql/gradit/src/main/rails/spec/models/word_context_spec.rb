require 'spec_helper'

describe WordContext do
  before(:each) do
    @w = Word.createNew(1, "vex", "definition", "wordlist")
  end

  it "should create a new instance given valid attributes" do
    WordContext.createNew(@w.wordid, "Book Name", 5, "This is the line.").should_not == nil
  end
end
