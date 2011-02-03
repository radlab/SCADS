class UsersController < ApplicationController
  def new
  end
  
  def create
    if params[:login] != "" and params[:password] != ""
      puts params[:login] + " " + params[:password] + "" + params[:name]
      u = User.createNew(params[:login], params[:password], params[:name])
      puts "**CREATED USER**"
      puts u.login
      
      session[:user] = params[:login]
      flash[:notice] = "Successfully created user."
      redirect_to :controller => :games
      return
    end
    
    render 'users/new'
  end
  
  def login
  end
  
  def login_action
    user = params[:login]
    pass = params[:password]
    
    u = User.find(user)
    puts "**USER LOGGING IN**"
    puts u.login
    
    if u and u.password == pass #Successful login
      session[:user] = u
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
