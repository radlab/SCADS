require 'spec_helper'

describe Thought do
  before(:each) do
    @valid_attributes = {
      :owner => "value for owner",
      :timestamp => 1,
      :text => "value for text"
    }
  end

  it "should create a new instance given valid attributes" do
    Thought.create!(@valid_attributes)
  end
end
