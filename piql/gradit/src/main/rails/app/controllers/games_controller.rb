class GamesController < ApplicationController
  
  before_filter :login_required, :except => [:index]
  before_filter :valid_game?, :only => [:game_entry, :ans]
  
  def index #TODO: Remove
    @current_user = current_user
    @games = []
    @wordlists = WordList.all
  end
 
  #Check if the answer was correct
  def ans
    @current_user = current_user
    game = Game.find(params[:id].to_i)
    #Find currentword in the game and answer chosen  
    choice = params[:answer]
    answer = game.answer
    
  	gp = GamePlayer.find(game.gameid, current_user)

    if choice == answer.word #If correct answer
      #Raise score
      gp.incrementScore(10)
      #Pick a new "current" word from the wordlist 
      if game.hasNextWord
        game.changeWord
      else #No more words in the game
        game.quit
        flash[:notice] = "You've finished the game! Your score was " + gp.score.to_s + "."
        redirect_to dashboard_path
        return
      end
      
      flash[:correct] = "Correct! (+10 points)"
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
    else #Incorrect answer
      #Lower score 
      gp.incrementScore(-5)
     
      w = Word.find_by_word(choice)
      flash[:incorrect] = "Oops, that's wrong. (<b>" + choice + "</b> means " + w.definition + ")"
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
    end
  end
  
  #Displaying/picking questions
  def game_entry
    @current_user = current_user
    game = Game.find(params[:id].to_i)
  	word = Word.find(game.currentword)
  	gp = GamePlayer.find(game.gameid, current_user)
  	@score = gp.score
    @wordsLeft = game.numWordsLeft
    #Get a random context for the word
    @para = false
    con = word.getContext #get context
  	if con != nil
  	  #Initialize paragraph, multiple choice settings
  	  @para_book = con.book;
      @para = con.wordLine
      @para.gsub!(/\b#{word.word}\b/i, '___________') #underline the missing word    
      puts "hi"
      @mc = game.choices(word.word)

      @mc_array = (@mc << word.word).shuffle
      puts "six"
    else #Find another word to use, no contexts
      if game.hasNextWord
        game.changeWord
        redirect_to :controller => :games, :action => :game_entry, :id => game.gameid
      else
        game.quit
        flash[:notice] = "You've finished the game! Your score was " + @score.to_s + "."
        redirect_to dashboard_path
      end
    end    
  end

  def new_game
    wordlist = WordList.find(params[:wordlist])
    
    if wordlist == nil or wordlist.words.empty?
      flash[:notice] = "That wordlist is invalid (doesn't exist or has no words)"
      redirect_to dashboard_path
      return
    end
    
    game = Game.createNew(wordlist.name)
    gp = GamePlayer.createNew(game.gameid, current_user)

    if(game.hasNextWord) #If there is a word
      currentword = game.changeWord
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
      return
    end
    flash[:notice] = "Wordlist has no words!"
    redirect_to :back
  end

  def quit_game
    Game.find(params[:id]).quit
    redirect_to dashboard_path
  end
  
  def challenge
    @user = params[:id]
    @wordlists = User.find("admin").wordlists
  end

  def challenge_user
    user2 = params[:user]
    user1 = current_user
    
    wordlist = WordList.find(params[:wordlist])

    if wordlist == nil or wordlist.words.empty?
      flash[:notice] = "That wordlist is invalid (doesn't exist or has no words)"
      redirect_to dashboard_path
      return
    end
    
    game1 = Game.createNew(wordlist.name, 1)
    game2 = Game.createNew(wordlist.name, 1)

    gp1 = GamePlayer.createNew(game1.gameid, user1)
    gp2 = GamePlayer.createNew(game2.gameid, user2)
    
    challenge = Challenge.createNew(user1, user2, game1.gameid, game2.gameid)

    if(game1.hasNextWord) #If there is a word
      currentword = game1.changeWord
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game1.gameid)
      return
    end
    flash[:notice] = "Wordlist has no words!"
    redirect_to :back
  end
  private
  
  def valid_game?
    if !Game.valid_game?(params[:id], current_user)
      flash[:notice] = "Sorry, that game isn't valid."
      redirect_to :games
    end
  end
end
