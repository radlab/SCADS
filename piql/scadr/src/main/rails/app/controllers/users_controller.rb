class UsersController < ApplicationController

  # def index
  # end

  def new
    @user = User.new
  end

  def create
    @user = User.new(params[:user])
    # FIXME: Why do I need to do this?
    @user.username = params[:user][:username]
    
    if params[:user][:password] != params[:confirm_password]
      @user.errors.push [:password, "does not match"]
      render :action => :new
    elsif @user.password.blank?
      @user.errors.push [:password, "cannot be blank"]
      render :action => :new
    else
      @user.password = Digest::MD5.hexdigest(params[:user][:password])
      if @user.save
        flash[:notice] = "Your account \"#{@user.username}\" has been created!"
        redirect_to root_path
      else
        @user.password = params[:user][:password]
        render :action => :new
      end
    end
  end

  def show
    raw_users = User.find_user(params[:id])
    raw_users.present? ? @user = raw_users.first.first : @user = nil
    
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
end
