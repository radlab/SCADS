require 'spec_helper'

describe Candidate do
  before(:each) do
    @valid_attributes = {
      :email => "bob@bob.com",
      :name => "Bob Bobfield",
      :research_area => "Systems",
      :school => "Barkeley",
      :gpa => 4.00
    }
  end
  
  it "should be able to save a candidate object" do
    cd = Candidate.new(@valid_attributes)
    cd.save.should be_true
  end
end