class UsersController < ApplicationController
  
  before_filter :login_required, :only => [:dashboard]
  
  def new
  end
  
  def dashboard
    @current_user = current_user
    user = User.find(current_user)
    games = user.games
    @unfinished_games = games.select {|g| g.done == 0}  
    @finished_games = games.select {|g| g.done == 1}  
    @wordlists = User.find("admin").wordlists.concat user.wordlists #TODO: OPTIMIZE
    @leaderboard = User.get_leaderboard
    
    challenges = user.challenges
    @finished_challenges = challenges.select {|c| c.done == 1}
    challenges = challenges.select {|c| c.done == 0}

    #Challenges that you still have to do
    @unfinished_challenges = []

    for ch in challenges
        if ch.user1 == current_user
            @unfinished_challenges << ch if Game.find(ch.game1).done == 0
        else
            @unfinished_challenges << ch if Game.find(ch.game2).done == 0
        end
    end

    #Waiting for the other person
    @pending_challenges = []
     
    for ch in challenges
        if ch.user1 == current_user
            @pending_challenges << ch if Game.find(ch.game2).done == 0 and Game.find(ch.game1).done == 1
        else
            @pending_challenges << ch if Game.find(ch.game1).done == 0 and Game.find(ch.game2).done == 1
        end
    end
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
    redirect_to dashboard_path
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
    redirect_to :controller => :home 
  end
  
end
