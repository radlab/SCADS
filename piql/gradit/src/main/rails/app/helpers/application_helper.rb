# Methods added to this helper will be available to all templates in the application.
module ApplicationHelper
  def current_user
    return session[:user]
  end
end
