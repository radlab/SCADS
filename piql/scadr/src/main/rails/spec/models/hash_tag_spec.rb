require 'spec_helper'

describe HashTag do
  before(:each) do
    @valid_attributes = {
      :tag => "value for tag",
      :timestamp => 1,
      :owner => "value for owner"
    }
  end

  it "should create a new instance given valid attributes" do
    HashTag.create!(@valid_attributes)
  end
end
