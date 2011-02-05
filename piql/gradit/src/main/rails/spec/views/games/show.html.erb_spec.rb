require 'spec_helper'

describe "/wordlists/show.html.erb" do
  include WordlistsHelper
  before(:each) do
    assigns[:wordlist] = @wordlist = stub_model(Wordlist,
      :string => 
    )
  end

  it "renders attributes in <p>" do
    render
    response.should have_text(//)
  end
end
