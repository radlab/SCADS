require 'spec_helper'

describe User do
  before(:each) do
    @valid_attributes = {
      :username => "Kamina",
      :home_town => "Bro Town",
      :plain_password => "GurreN",
      :confirm_password => "GurreN"
    }
  end

  it "should create a new instance given valid attributes" do
    User.create!(@valid_attributes).should_not be_nil
  end
  
  it "should not create a new instance given invalid password" do
    user = User.new(@valid_attributes.merge(:confirm_password => "derp"))
    user.save.should be_false
    
    user = User.new(@valid_attributes.merge(:plain_password => "", :confirm_password => ""))
    user.save.should be_false
  end
  
  it "should add errors when validating password" do
    user = User.new(@valid_attributes.merge(:confirm_password => "derp"))
    user.valid_password?.should be_false
    user.errors.should be_present
    
    user = User.new(@valid_attributes.merge(:plain_password => "", :confirm_password => ""))
    user.valid_password?.should be_false
    user.errors.should be_present
  end
  
  it "should save users to the database" do
    user = User.new(@valid_attributes)
    user.save.should be_true
    User.find(user.username).should_not be_nil
  end
  
  it "should be able to compare users" do
    user = User.create!(@valid_attributes)
    found_user = User.find(user.username)
    user.should_not be_nil
    found_user.should_not be_nil
    user.should == found_user
  end
  
  it "should clear the database when tests complete" do
    User.find("Kamina").should be_nil
  end
end