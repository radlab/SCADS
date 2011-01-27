require 'spec_helper'

describe "/words/show.html.erb" do
  include WordsHelper
  before(:each) do
    assigns[:word] = @word = stub_model(Word,
      :string => 
    )
  end

  it "renders attributes in <p>" do
    render
    response.should have_text(//)
  end
end
