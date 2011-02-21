class InterviewsController < ApplicationController
  def show
    @candidate = Candidate.find(Candidate.unescape(params[:candidate_id]))
    @interview = Interview.find(@candidate.email, params[:id].to_i)
  end

  def edit
    @candidate = Candidate.find(Candidate.unescape(params[:candidate_id]))
    @interview = Interview.find(@candidate.email, params[:id].to_i)
  end

  def update
  end
end
