require 'spec_helper'

describe WordsController do

  def mock_word(stubs={})
    @mock_word ||= mock_model(Word, stubs)
  end

  describe "GET index" do
    it "assigns all words as @words" do
      Word.stub(:find).with(:all).and_return([mock_word])
      get :index
      assigns[:words].should == [mock_word]
    end
  end

  describe "GET show" do
    it "assigns the requested word as @word" do
      Word.stub(:find).with("37").and_return(mock_word)
      get :show, :id => "37"
      assigns[:word].should equal(mock_word)
    end
  end

  describe "GET new" do
    it "assigns a new word as @word" do
      Word.stub(:new).and_return(mock_word)
      get :new
      assigns[:word].should equal(mock_word)
    end
  end

  describe "GET edit" do
    it "assigns the requested word as @word" do
      Word.stub(:find).with("37").and_return(mock_word)
      get :edit, :id => "37"
      assigns[:word].should equal(mock_word)
    end
  end

  describe "POST create" do

    describe "with valid params" do
      it "assigns a newly created word as @word" do
        Word.stub(:new).with({'these' => 'params'}).and_return(mock_word(:save => true))
        post :create, :word => {:these => 'params'}
        assigns[:word].should equal(mock_word)
      end

      it "redirects to the created word" do
        Word.stub(:new).and_return(mock_word(:save => true))
        post :create, :word => {}
        response.should redirect_to(word_url(mock_word))
      end
    end

    describe "with invalid params" do
      it "assigns a newly created but unsaved word as @word" do
        Word.stub(:new).with({'these' => 'params'}).and_return(mock_word(:save => false))
        post :create, :word => {:these => 'params'}
        assigns[:word].should equal(mock_word)
      end

      it "re-renders the 'new' template" do
        Word.stub(:new).and_return(mock_word(:save => false))
        post :create, :word => {}
        response.should render_template('new')
      end
    end

  end

  describe "PUT update" do

    describe "with valid params" do
      it "updates the requested word" do
        Word.should_receive(:find).with("37").and_return(mock_word)
        mock_word.should_receive(:update_attributes).with({'these' => 'params'})
        put :update, :id => "37", :word => {:these => 'params'}
      end

      it "assigns the requested word as @word" do
        Word.stub(:find).and_return(mock_word(:update_attributes => true))
        put :update, :id => "1"
        assigns[:word].should equal(mock_word)
      end

      it "redirects to the word" do
        Word.stub(:find).and_return(mock_word(:update_attributes => true))
        put :update, :id => "1"
        response.should redirect_to(word_url(mock_word))
      end
    end

    describe "with invalid params" do
      it "updates the requested word" do
        Word.should_receive(:find).with("37").and_return(mock_word)
        mock_word.should_receive(:update_attributes).with({'these' => 'params'})
        put :update, :id => "37", :word => {:these => 'params'}
      end

      it "assigns the word as @word" do
        Word.stub(:find).and_return(mock_word(:update_attributes => false))
        put :update, :id => "1"
        assigns[:word].should equal(mock_word)
      end

      it "re-renders the 'edit' template" do
        Word.stub(:find).and_return(mock_word(:update_attributes => false))
        put :update, :id => "1"
        response.should render_template('edit')
      end
    end

  end

  describe "DELETE destroy" do
    it "destroys the requested word" do
      Word.should_receive(:find).with("37").and_return(mock_word)
      mock_word.should_receive(:destroy)
      delete :destroy, :id => "37"
    end

    it "redirects to the words list" do
      Word.stub(:find).and_return(mock_word(:destroy => true))
      delete :destroy, :id => "1"
      response.should redirect_to(words_url)
    end
  end

end
