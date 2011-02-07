require 'spec_helper'

describe User do
  before(:all) do
    @u = User.createNew("amber", "password", "Amber Feng")
  end

  it "should create a new instance given valid attributes" do
    @u.should_not == nil
  end
  
  it "should not create a new instance with missing attributes (besides name)" do
    
    # Should be nil
    User.createNew("", "thisisapassword", "My Name").should == nil
    User.createNew("myuser", "thisisanotherpassword", "Another Name").should == nil
    User.createNew("", "", "What's This").should == nil
    
    # Should not be nil
    User.createNew("missingattributes", "notmissingpassword", "").should_not == nil
  end
  
  it "should validate uniqueness of usernames" do
    User.createNew("amber", "passwordtwo", "Gnef Rebma").should == nil
  end
  
  it "should store passwords in an appropriate hash format which can later be recovered accurately" do
    # Passwords are not yet stored in a hash format (they are plaintext)
    # Hashed passwords shouldn't be able to be recovered... I don't understand this test?
  end
  
  it "should be able to find User by PK" do
    User.find("amber").login.should == "amber"
  end

end