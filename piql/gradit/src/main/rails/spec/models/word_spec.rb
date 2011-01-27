require 'spec_helper'

describe Word do
  before(:each) do
    @valid_attributes = {
      :string => 
    }
  end

  it "should create a new instance given valid attributes" do
    Word.create!(@valid_attributes)
  end
end
