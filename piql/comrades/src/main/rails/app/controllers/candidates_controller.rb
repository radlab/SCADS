class CandidatesController < ApplicationController
  def index
    @research_area = params[:research_area]
    @research_area ||= "systems"
    @waiting = Candidate.waiting(@research_area)
    @top_rated = Candidate.top_rated(@research_area)
  end
  
  def show
    @candidate = Candidate.find(Candidate.unescape(params[:id]))
    @interviews = @candidate.interviews
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
        :score => 0,
        :comments => "",
        :interviewer => "",
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
