require 'spec_helper'

describe UsersController do
  describe "routing" do
    it "recognizes and generates #new" do
      { :get => "/register" }.should route_to(:controller => "users", :action => "new")
    end

    it "recognizes and generates #login" do
      { :get => "/login" }.should route_to(:controller => "users", :action => "login")
    end

    it "recognizes and generates #logout" do
      { :get => "/logout" }.should route_to(:controller => "users", :action => "logout")
    end
  end
end
