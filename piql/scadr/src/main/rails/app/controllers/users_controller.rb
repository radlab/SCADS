class UsersController < ApplicationController
  before_filter :check_valid_user, :only => [:show]

  def new
    @user = User.new
  end

  def create
    @user = User.new(params[:user])
    
    if @user.save
      flash[:notice] = "Your account \"#{@user.username}\" has been created!"
      redirect_to root_path
    else
      render :action => :new
    end
  end

  def show
    raw_thoughts = @user.my_thoughts(10)
    @thoughts = raw_thoughts.collect{ |t| t.first }
    
    raw_thoughtstream = @user.thoughtstream(10)
    # Remember that this comes in the form of (subscription, thought)
    # TODO: Currently no worky
    @thoughtstream = raw_thoughtstream.collect{ |ts| ts[1] }
    
    @can_subscribe = current_user && current_user != @user
    
    if @can_subscribe
      # Subscriptions come in this form: (subscription, userkeytype, user)
      raw_subscriptions = current_user.following(10)
      raw_subscriptions.each do |raw_sub|
        sub = raw_sub.first
        if sub.target == @user.username
          @can_subscribe = false
          break
        end
      end
    end
  end
  
  private
    def check_valid_user
      @user = User.find(params[:id])
      if @user.blank?
        flash[:notice] = "The user #{params[:id].to_s} does not exist."
        redirect_to root_path
      end
    end
end
