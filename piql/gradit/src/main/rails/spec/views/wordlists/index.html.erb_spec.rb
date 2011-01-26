require 'spec_helper'

describe "/wordlists/index.html.erb" do
  include WordlistsHelper

  before(:each) do
    assigns[:wordlists] = [
      stub_model(Wordlist,
        :string => 
      ),
      stub_model(Wordlist,
        :string => 
      )
    ]
  end

  it "renders a list of wordlists" do
    render
    response.should have_tag("tr>td", .to_s, 2)
  end
end
