require 'spec_helper'

describe User do
  before(:each) do
    
  end

  it "should create a new instance given valid attributes" do
    User.createNew("amber", "password", "Amber Feng").should_not == nil
  end
  
  it "should not create a new instance with missing attributes (besides name)" do
  end
  
  it "should validate uniqueness of usernames" do
  end
  
  it "should store passwords in an appropriate hash format which can later be recovered accurately" do
  end
  
  it "should be able to find User by PK" do
  end

end