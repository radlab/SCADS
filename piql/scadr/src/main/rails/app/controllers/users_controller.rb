class UsersController < ApplicationController
  before_filter :check_valid_user, :only => [:show]

  def index
    @users = $EXAMPLE_USERS
  end

  def new
    @user = User.new
  end

  def create
    @user = User.new(params[:user])
    @user.username = helpers.strip_tags(@user.username)
    @user.home_town = helpers.strip_tags(@user.home_town)
    
    if @user.save
      flash[:notice] = "Your account \"#{@user.username}\" has been created!"
      @user_session = UserSession.new(:username => params[:user][:username], :password => params[:user][:plain_password])
      if @user_session.save
        session[:username] = @user_session.username
        redirect_to :controller => :users, :action => :show, :id => @user_session.username
      else
        redirect_to root_path
      end
    else
      render :action => :new
    end
  end

  def show
    @thoughts = @user.my_thoughts(10)
    @thoughtstream = @user.thoughtstream(10)
    @followed = @user.users_followed(10)
    
    if current_user && current_user != @user
      @subscription = Subscription.find(current_user.username, @user.username)
    else
      @subscription = nil
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
