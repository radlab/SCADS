class CandidatesController < ApplicationController
  def index
    @waiting = Candidate.waiting("systems")
    @top_rated = Candidate.top_rated("systems")
  end
  
  def show
    @candidate = Candidate.find(params[:id])
  end
  
  def new
    @candidate = Candidate.new
  end
  
  def create
    params[:candidate][:gpa] = params[:candidate][:gpa].to_f
    @candidate = Candidate.new(params[:candidate])
    if @candidate.save
      @interview = Interview.new({
        :candidate => @candidate.email,
        :created_at => Time.now.to_i,
        :interviewed_at => 0,
        :status => "PENDING",
        :score => nil,
        :comments => nil,
        :interviewer => nil,
        :research_area => @candidate.research_area
        })
      if @interview.save
        flash[:notice] = "Candidate profile added."
        redirect_to :action => :index
      else
        flash[:error] = "Failed to set interview status."
        redirect_to :action => :index
      end
    else
      flash[:error] = "Failed to create candidate profile."
      render :action => :new
    end
  end
end
