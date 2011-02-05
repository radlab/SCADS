require 'spec_helper'

describe GamesController do
  describe "routing" do
    it "recognizes and generates #index" do
      { :get => "/games" }.should route_to(:controller => "games", :action => "index")
    end

    it "recognizes new_game" do
      { :get => "/games/new_game/0?wordlist=wordlist" }.should route_to(:controller => "games", :action => "new_game", :id => 0, :wordlist => "wordlist")
    end

    it "recognizes game_entry" do
      { :get => "/games/game_entry/1" }.should route_to(:controller => "games", :action => "game_entry", :id => "1")
    end

    it "recognizes ans" do
      { :get => "/games/ans/1?answer=vex" }.should route_to(:controller => "games", :action => "ans", :id => "1", :answer => "vex")
    end

  end
end
