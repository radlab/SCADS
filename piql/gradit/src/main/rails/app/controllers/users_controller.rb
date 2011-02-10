class UsersController < ApplicationController
  
  def index
  end
  
  def new
  end
  
  def create
    u = User.createNew(params[:login], params[:password], params[:name])
    
    if u == nil #Username is already taken
      flash[:notice] = "There was a problem with your registration."
      render 'users/new'
      return
    end
    
    puts "**CREATED USER**"
    puts u.login
    
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
    
    puts "are you logging in?"
    
    u = User.login(user, pass)

    if u != nil
      session[:user] = u.login
      flash[:notice] = "You've been successfully logged in, " + u.login + "!"
      redirect_to :controller => :games
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
