require 'spec_helper'

describe WordlistsController do

  def mock_wordlist(stubs={})
    @mock_wordlist ||= mock_model(Wordlist, stubs)
  end

  describe "GET index" do
    it "assigns all wordlists as @wordlists" do
      Wordlist.stub(:find).with(:all).and_return([mock_wordlist])
      get :index
      assigns[:wordlists].should == [mock_wordlist]
    end
  end

  describe "GET show" do
    it "assigns the requested wordlist as @wordlist" do
      Wordlist.stub(:find).with("37").and_return(mock_wordlist)
      get :show, :id => "37"
      assigns[:wordlist].should equal(mock_wordlist)
    end
  end

  describe "GET new" do
    it "assigns a new wordlist as @wordlist" do
      Wordlist.stub(:new).and_return(mock_wordlist)
      get :new
      assigns[:wordlist].should equal(mock_wordlist)
    end
  end

  describe "GET edit" do
    it "assigns the requested wordlist as @wordlist" do
      Wordlist.stub(:find).with("37").and_return(mock_wordlist)
      get :edit, :id => "37"
      assigns[:wordlist].should equal(mock_wordlist)
    end
  end

  describe "POST create" do

    describe "with valid params" do
      it "assigns a newly created wordlist as @wordlist" do
        Wordlist.stub(:new).with({'these' => 'params'}).and_return(mock_wordlist(:save => true))
        post :create, :wordlist => {:these => 'params'}
        assigns[:wordlist].should equal(mock_wordlist)
      end

      it "redirects to the created wordlist" do
        Wordlist.stub(:new).and_return(mock_wordlist(:save => true))
        post :create, :wordlist => {}
        response.should redirect_to(wordlist_url(mock_wordlist))
      end
    end

    describe "with invalid params" do
      it "assigns a newly created but unsaved wordlist as @wordlist" do
        Wordlist.stub(:new).with({'these' => 'params'}).and_return(mock_wordlist(:save => false))
        post :create, :wordlist => {:these => 'params'}
        assigns[:wordlist].should equal(mock_wordlist)
      end

      it "re-renders the 'new' template" do
        Wordlist.stub(:new).and_return(mock_wordlist(:save => false))
        post :create, :wordlist => {}
        response.should render_template('new')
      end
    end

  end

  describe "PUT update" do

    describe "with valid params" do
      it "updates the requested wordlist" do
        Wordlist.should_receive(:find).with("37").and_return(mock_wordlist)
        mock_wordlist.should_receive(:update_attributes).with({'these' => 'params'})
        put :update, :id => "37", :wordlist => {:these => 'params'}
      end

      it "assigns the requested wordlist as @wordlist" do
        Wordlist.stub(:find).and_return(mock_wordlist(:update_attributes => true))
        put :update, :id => "1"
        assigns[:wordlist].should equal(mock_wordlist)
      end

      it "redirects to the wordlist" do
        Wordlist.stub(:find).and_return(mock_wordlist(:update_attributes => true))
        put :update, :id => "1"
        response.should redirect_to(wordlist_url(mock_wordlist))
      end
    end

    describe "with invalid params" do
      it "updates the requested wordlist" do
        Wordlist.should_receive(:find).with("37").and_return(mock_wordlist)
        mock_wordlist.should_receive(:update_attributes).with({'these' => 'params'})
        put :update, :id => "37", :wordlist => {:these => 'params'}
      end

      it "assigns the wordlist as @wordlist" do
        Wordlist.stub(:find).and_return(mock_wordlist(:update_attributes => false))
        put :update, :id => "1"
        assigns[:wordlist].should equal(mock_wordlist)
      end

      it "re-renders the 'edit' template" do
        Wordlist.stub(:find).and_return(mock_wordlist(:update_attributes => false))
        put :update, :id => "1"
        response.should render_template('edit')
      end
    end

  end

  describe "DELETE destroy" do
    it "destroys the requested wordlist" do
      Wordlist.should_receive(:find).with("37").and_return(mock_wordlist)
      mock_wordlist.should_receive(:destroy)
      delete :destroy, :id => "37"
    end

    it "redirects to the wordlists list" do
      Wordlist.stub(:find).and_return(mock_wordlist(:destroy => true))
      delete :destroy, :id => "1"
      response.should redirect_to(wordlists_url)
    end
  end

end
