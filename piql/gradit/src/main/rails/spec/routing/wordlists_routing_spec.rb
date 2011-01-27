require 'spec_helper'

describe WordlistsController do
  describe "routing" do
    it "recognizes and generates #index" do
      { :get => "/wordlists" }.should route_to(:controller => "wordlists", :action => "index")
    end

    it "recognizes and generates #new" do
      { :get => "/wordlists/new" }.should route_to(:controller => "wordlists", :action => "new")
    end

    it "recognizes and generates #show" do
      { :get => "/wordlists/1" }.should route_to(:controller => "wordlists", :action => "show", :id => "1")
    end

    it "recognizes and generates #edit" do
      { :get => "/wordlists/1/edit" }.should route_to(:controller => "wordlists", :action => "edit", :id => "1")
    end

    it "recognizes and generates #create" do
      { :post => "/wordlists" }.should route_to(:controller => "wordlists", :action => "create") 
    end

    it "recognizes and generates #update" do
      { :put => "/wordlists/1" }.should route_to(:controller => "wordlists", :action => "update", :id => "1") 
    end

    it "recognizes and generates #destroy" do
      { :delete => "/wordlists/1" }.should route_to(:controller => "wordlists", :action => "destroy", :id => "1") 
    end
  end
end
