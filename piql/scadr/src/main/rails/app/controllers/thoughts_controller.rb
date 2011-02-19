class ThoughtsController < ApplicationController
  def new
    @owner = User.find_user(params[:user_id])
    @thought = Thought.new
  end

  def create
    @thought = Thought.new params[:thought]
    @thought.owner = current_user.username
    @thought.timestamp = Time.now.to_i
    @thought.text = helpers.sanitize(@thought.text)
    if @thought.save
      flash[:notice] = "New thought created."
      redirect_to user_path(current_user)
    else
      flash[:error] = "Save failed."
      render :action => :new
    end
  end

  def edit
  end
  
  def thoughtstream
    @user = User.find(params[:user_id])
    @thoughtstream = @user.thoughtstream(10)
  end
  
  def thoughts
    @user = User.find(params[:user_id])
    @thoughts = @user.my_thoughts(10)
  end
end
