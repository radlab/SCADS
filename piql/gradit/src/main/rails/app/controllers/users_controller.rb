class UsersController < ApplicationController
  
  before_filter :login_required, :only => [:dashboard]
  
  def new
  end
  
  def dashboard
    @current_user = current_user

    games = User.find(current_user).games
    puts games
    @unfinished_games = games.select {|g| g.done == 0}  
    @finished_games = games.select {|g| g.done == 1}  
    @wordlists = User.find(current_user).wordlists
    @leaderboard = User.get_leaderboard
  end
  
  def create
    u = User.createNew(params[:login], params[:password], params[:name])
    
    if u == nil #Username is already taken
      flash[:notice] = "There was a problem with your registration."
      render 'users/new'
      return
    end
    
    session[:user] = params[:login]
    flash[:notice] = "Successfully created user."
    redirect_to :controller => :games
    return

  end
  
  def login
  end
  
  def login_action
    user = params[:login]
    pass = params[:password]
    
    u = User.login(user, pass)

    if u != nil
      session[:user] = u.login
      flash[:notice] = "You've been successfully logged in, " + u.login + "!"
      redirect_to dashboard_path 
    else #Unsuccessful login
      flash[:notice] = "Unsuccessful login."
      redirect_to "/login"
    end
    
  end
  
  def logout
    session[:user] = nil
    
    flash[:notice] = "You've been logged out."
    redirect_to :controller => :games
  end
  
end
