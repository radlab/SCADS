require 'spec_helper'

describe "/wordlists/new.html.erb" do
  include WordlistsHelper

  before(:each) do
    assigns[:wordlist] = stub_model(Wordlist,
      :new_record? => true,
      :string => 
    )
  end

  it "renders new wordlist form" do
    render

    response.should have_tag("form[action=?][method=post]", wordlists_path) do
      with_tag("input#wordlist_string[name=?]", "wordlist[string]")
    end
  end
end
