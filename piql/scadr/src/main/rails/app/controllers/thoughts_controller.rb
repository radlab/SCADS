class ThoughtsController < ApplicationController
  def new
    query = User.find_user(params[:user_id])
    @owner = query.present? ? query.first.first : nil
    @thought = Thought.new
  end

  def create
    @thought = Thought.new params[:thought]
    @thought.owner = current_user.username
    @thought.timestamp = Time.now.to_i
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
    raw_users = User.find_user(params[:user_id])
    raw_users.present? ? @user = raw_users.first.first : @user = nil

    raw_thoughtstream = @user.thoughtstream(10)
    # Remember that this comes in the form of (subscription, thought)
    # TODO: Currently no worky
    @thoughtstream = raw_thoughtstream.collect{ |ts| ts[1] }
  end
  
  def thoughts
    raw_users = User.find_user(params[:user_id])
    raw_users.present? ? @user = raw_users.first.first : @user = nil

    raw_thoughts = @user.my_thoughts(10)
    @thoughts = raw_thoughts.collect{ |t| t.first }
  end

  # def show
  #   @owner = User.find_user(params[:user_id])
  #   @thought = Thought.find_thought(params[:user_id], params[:id])
  # end

end
