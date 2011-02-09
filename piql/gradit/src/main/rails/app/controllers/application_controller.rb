# Filters added to this controller apply to all controllers in the application.
# Likewise, all the methods added will be available for all controllers.

class ApplicationController < ActionController::Base
  helper :all # include all helpers, all the time
  #protect_from_forgery # See ActionController::RequestForgeryProtection for details
  #disable forgery protection so rain works
  self.allow_forgery_protection = false
	

  # Scrub sensitive parameters from your log
  # filter_parameter_logging :password
  
  def current_user
    return session[:user] if session[:user]
    #return User.guest_user.login
  end
  
  def login_required
    if current_user == nil
        flash[:notice] = "You must be logged in to play a game."
        redirect_to :controller => :games
        return false
    end
    return true
  end
  
end
