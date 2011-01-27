require 'spec_helper'

describe "/words/edit.html.erb" do
  include WordsHelper

  before(:each) do
    assigns[:word] = @word = stub_model(Word,
      :new_record? => false,
      :string => 
    )
  end

  it "renders the edit word form" do
    render

    response.should have_tag("form[action=#{word_path(@word)}][method=post]") do
      with_tag('input#word_string[name=?]', "word[string]")
    end
  end
end
