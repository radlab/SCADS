require 'spec_helper'

describe Subscription do
  before(:each) do
    @valid_attributes = {
      :owner => "value for owner",
      :target => "value for target"
    }
  end

  it "should create a new instance given valid attributes" do
    Subscription.create!(@valid_attributes)
  end
end
