require 'spec_helper'

describe Book do
  before(:each) do
    @valid_attributes = {
      :id => 1,
      :name => "value for name"
    }
  end

  it "should create a new instance given valid attributes" do
    Book.create!(@valid_attributes)
  end
end
