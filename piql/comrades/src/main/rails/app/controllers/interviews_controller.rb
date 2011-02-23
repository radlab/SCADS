class InterviewsController < ApplicationController
  def show
    @candidate = Candidate.find(Candidate.unescape(params[:candidate_id]))
    @interview = Interview.find(@candidate.email, params[:id].to_i)
  end

  def edit
    @candidate = Candidate.find(Candidate.unescape(params[:candidate_id]))
    @interview = Interview.find(@candidate.email, params[:id].to_i)
    @interview.new_record = false
  end

  def update
    @candidate = Candidate.find(Candidate.unescape(params[:candidate_id]))
    @interview = Interview.find(@candidate.email, params[:id].to_i)
    
    params[:interview][:score] = params[:interview][:score].to_i
    
    params[:interview].each do |key, value|
      value = helpers.strip_tags(value) if value.respond_to?(:empty?)
      @interview.send "#{key.to_s}=".to_sym, value
    end

    errors = []    
    [:interviewer, :comments, :score].each do |required|
      if params[:interview][required].blank?
        errors.push required
      end
    end
    
    if errors.blank?
      @interview.interviewed_at = Time.now.to_i
      @interview.status = "INTERVIEWED"
      if @interview.save
        flash[:notice] = "Candidate interviewed."
        redirect_to candidate_interview_path(@candidate, @interview)
      else
        flash[:error] = "Failed to update interview."
        render :action => :edit
      end
    else
      flash[:error] = "Please fill out all fields with valid inputs."
      render :action => :edit
    end
  end
  
  def decide
    @candidate = Candidate.find(Candidate.unescape(params[:candidate_id]))
    @interview = Interview.find(@candidate.email, params[:id].to_i)

    if params[:interview][:decision] == "OFFER"
      @interview.status = "OFFERED"
      if @interview.save
        flash[:notice] = "Offer sent to candidate #{@candidate.name}."
        redirect_to root_path
      else
        flash[:notice] = "Failed to send offer."
        redirect_to candidate_interview_path(@candidate, @interview)
      end
    elsif params[:interview][:decision] == "REJECT"
      @interview.status = "REJECTED"
      if @interview.save
        flash[:notice] = "Rejected candidate #{@candidate.name}."
        redirect_to root_path
      else
        flash[:notice] = "Failed to send rejection."
        redirect_to candidate_interview_path(@candidate, @interview)
      end
    else
      flash[:error] = "Invalid decision."
      redirect_to candidate_interview_path(@candidate, @interview)
    end
  end
end
