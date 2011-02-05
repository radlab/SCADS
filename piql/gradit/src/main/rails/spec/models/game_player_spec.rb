require 'spec_helper'

describe GamePlayer do
  before(:each) do
    @g = Game.createNew("wordlist")
    @u = User.createNew("user", "pass", "User Name")
  end

  it "should create a new instance given valid attributes" do
    GamePlayer.createNew(@g.gameid, @u.login).should_not == nil
  end
  
  it "should find GamePlayer by PK" do 
  end
  
  it "should be able to increment a GamePlayer's score"
end
