require 'spec_helper'

describe BookLine do
  before(:each) do
    @valid_attributes = {
      :id => 1,
      :line => "value for line",
      :source => 
    }
  end

  it "should create a new instance given valid attributes" do
    BookLine.create!(@valid_attributes)
  end
end
