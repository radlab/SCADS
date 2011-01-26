require 'spec_helper'

describe "/words/new.html.erb" do
  include WordsHelper

  before(:each) do
    assigns[:word] = stub_model(Word,
      :new_record? => true,
      :string => 
    )
  end

  it "renders new word form" do
    render

    response.should have_tag("form[action=?][method=post]", words_path) do
      with_tag("input#word_string[name=?]", "word[string]")
    end
  end
end
