require 'spec_helper'

describe "/words/index.html.erb" do
  include WordsHelper

  before(:each) do
    assigns[:words] = [
      stub_model(Word,
        :string => 
      ),
      stub_model(Word,
        :string => 
      )
    ]
  end

  it "renders a list of words" do
    render
    response.should have_tag("tr>td", .to_s, 2)
  end
end
