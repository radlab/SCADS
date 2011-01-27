require 'spec_helper'

describe WordsController do
  describe "routing" do
    it "recognizes and generates #index" do
      { :get => "/words" }.should route_to(:controller => "words", :action => "index")
    end

    it "recognizes and generates #new" do
      { :get => "/words/new" }.should route_to(:controller => "words", :action => "new")
    end

    it "recognizes and generates #show" do
      { :get => "/words/1" }.should route_to(:controller => "words", :action => "show", :id => "1")
    end

    it "recognizes and generates #edit" do
      { :get => "/words/1/edit" }.should route_to(:controller => "words", :action => "edit", :id => "1")
    end

    it "recognizes and generates #create" do
      { :post => "/words" }.should route_to(:controller => "words", :action => "create") 
    end

    it "recognizes and generates #update" do
      { :put => "/words/1" }.should route_to(:controller => "words", :action => "update", :id => "1") 
    end

    it "recognizes and generates #destroy" do
      { :delete => "/words/1" }.should route_to(:controller => "words", :action => "destroy", :id => "1") 
    end
  end
end
