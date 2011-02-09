require 'spec_helper'

describe Thought do
  before(:each) do
    @user = User.create!(
      :username => "Kamina",
      :home_town => "Bro Town",
      :plain_password => "GurreN",
      :confirm_password => "GurreN"
    )
    @valid_attributes = {
      :owner => "Kamina",
      :timestamp => Time.now.to_i,
      :text => "Don't believe in yourself! Believe in me, who believes in you!"
    }
  end

  it "should create a new instance given valid attributes" do
    Thought.create!(@valid_attributes)
  end
  
  it "should have a human-readable timestamp" do
    thought = Thought.create!(@valid_attributes.merge(:timestamp => Time.now.to_i))
    thought.posted_at.should == "0 minutes ago"
    thought.timestamp = 2.hours.ago.to_i
    thought.posted_at.should == "2 hours ago"
    thought.timestamp = 2.days.ago.to_i
    thought.posted_at.should include(Time.at(thought.timestamp).day.to_s)
  end
end
