require 'spec_helper'

describe GamePlayer do
  before(:all) do
    @g = Game.createNew("wordlist")
    @u = User.createNew("user", "pass", "User Name")
    @gp = GamePlayer.createNew(@g.gameid, @u.login)
  end

  it "should create a new instance given valid attributes" do
    @gp.should_not == nil
  end
  
  it "should find GamePlayer by PK" do 
    GamePlayer.find(@g.gameid, @u.login).should_not == nil
  end
  
  it "should be able to increment a GamePlayer's score" do
    @gp.score = 100
    @gp.save
    @gp.score.should == 100
    
    @gp.incrementScore(42)
    @gp.save
    @gp.score.should == 142
  end
end
