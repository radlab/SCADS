require 'spec_helper'

describe "/wordlists/edit.html.erb" do
  include WordlistsHelper

  before(:each) do
    assigns[:wordlist] = @wordlist = stub_model(Wordlist,
      :new_record? => false,
      :string => 
    )
  end

  it "renders the edit wordlist form" do
    render

    response.should have_tag("form[action=#{wordlist_path(@wordlist)}][method=post]") do
      with_tag('input#wordlist_string[name=?]', "wordlist[string]")
    end
  end
end
