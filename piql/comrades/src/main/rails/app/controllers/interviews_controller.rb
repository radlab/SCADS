class InterviewsController < ApplicationController
  def edit
    @candidate = Candidate.find(params[:candidate_id])
    @interview = Interview.find(params[:candidate_id], params[:id])
  end
  
  def update
  end
end
