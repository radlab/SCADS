class WordlistsController < ApplicationController
  before_filter :login_required
  def index
  end

  def new
  end
  
  def create
    w = WordList.createNew(params[:name], current_user)
    
    if w == nil #Wordlist name is already taken
      flash[:notice] = "Please choose a unique wordlist name."
      render 'wordlists/new'
      return
    end
    
    puts "**CREATED WORDLIST**"
    puts w.name
    
    flash[:notice] = "Successfully created wordlist."
    redirect_to dashboard_path       
    return

  end
  
end
