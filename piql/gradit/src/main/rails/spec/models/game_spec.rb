require 'spec_helper'

describe Game do
  before(:each) do
    @w = WordList.createNew("wordlist")
  end

  it "should create a new instance given valid attributes" do
    Game.createNew("wordlist").should_not == nil
  end
  
  it "should autoincrement the gameid upon creation" do
    g1 = Game.createNew("wordlist")
    g2 = Game.createNew("wordlist")
    
    (g2.gameid - g1.gameid).should == 1
  end
  
  it "should be able to find a Game by PK" do
    g = Game.createNew("wordlist")
    Game.find(g.gameid).should == g
  end
  
  it "should correctly find all Games (with a cardinality limit)" do
  end
  
  it "should run changeWord correctly" do
    g = Game.createNew("wordlist")
    
    g.changeWord(18)
    g.currentword.should == 18
  end
  
  it "should return the correct answer (current word)" do 
    g = Game.createNew("wordlist")
    w = Word.createNew(1, "vex", "definition", "wordlist")
    
    g.currentword = w.wordid
    g.answer.should == w
  end
  
end
