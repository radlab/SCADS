require 'spec_helper'

describe Interview do
  before(:each) do
    @candidate = Candidate.new({
      :email => "bob@bob.com",
      :name => "Bob Bobfield",
      :research_area => "Systems",
      :school => "Barkeley",
      :gpa => 4.00
    })
    @candidate.save
    @valid_attributes = {
      :candidate => "bob@bob.com",
      :created_at => 0,
      :interviewed_at => 0,
      :status => "PENDING",
      :score => 5,
      :comments => "",
      :interviewer => "Jesus",
      :research_area => "Systems"
    }
  end

  it "should be able to access a candidate's interviews" do
    iv = Interview.new(@valid_attributes)
    iv.save.should be_true
    interviews = Interview.find_interviews_for_candidate(iv.candidate)
    interviews.should_not be_blank
    interviews.first.first.status.should == "PENDING"
  end
  
  it "should be able to return candidates that need interviews" do
    iv = Interview.new(@valid_attributes)
    iv.save
    pending_candidates = Interview.find_waiting(iv.research_area)
    cand = pending_candidates.first.first
    cand.email.should == "bob@bob.com"
  end
  
  it "should be able to return top-rated candidates" do
    iv = Interview.new(@valid_attributes.merge(:status => "INTERVIEWED", :interviewed_at => 123456))
    iv.save.should be_true
    iv.status.should == "INTERVIEWED"
    iv.score.should == 5
    candidates = Interview.findTopRated(iv.research_area)
    candidates.should_not be_empty
    cand = candidates.first.first
    cand.email.should == "bob@bob.com"
  end
end