class GamesController < ApplicationController
  
  # GET /games
  # GET /games.xml
  def index
    @current_user = current_user
    #Note: .all does not yet work
    @games = Game.all
    @wordlists = WordList.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @games }
    end
  end

  # GET /games/1
  # GET /games/1.xml
 
  #Check if the answer was correct
  def ans
    @current_user = current_user
    puts "Inside ANS"
    game = Game.find(params[:id].to_i)
    #Find currentword in the game and answer chosen  
    choice = params[:answer]
    answer = game.answer
    
    user = User.find(current_user)
  	gp = GamePlayer.find(game.gameid, user.login)

    puts "@@@@AND THE ANSWER IS@@@@@@@@"
    if choice == answer.word #If correct answer
      puts "CORRECT"
      #Pick a new "current" word from the wordlist **NEED TO OPTIMIZE THIS**
      wordlist = WordList.find(game.wordlist)

      #words = wordlist.words 
      words = wordlist.words
      
      #Pick a random word next
      nextWord = words[rand(words.length())]
      game.changeWord(nextWord.wordid)
      
      #Raise score
      gp.incrementScore(10)
      
      flash[:notice] = "Correct!"
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
      #AJAX update page to reflect changes in score, let the user know they are correct
      #render :update do |page|
    	#  page[:ans_result].replace_html "Correct! Press next." #**NEED TO HAVE THIS REDIRECT, BUT IT DOESN'T WORK**
     	#  page[:player_score].replace_html "#{score}"
     	#  page[:player_score].highlight
     	#
      #  page["mult_choice_#{choice}"].replace_html "<b>#{choice} (definition: #{answer.definition})</b>"
      #end
      
    else #Incorrect answer
      puts "INCORRECT"
      
      #Lower score 
      gp.incrementScore(-5)
     
      flash[:notice] = "Oops, that's wrong"
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
      #AJAX update page to reflect changes in score, let user know they are incorrect
      #render :update do |page|
      #  page[:ans_result].replace_html "Wrong, try again!"
	    #  page[:player_score].replace_html "#{score}"
      #  page[:player_score].highlight
      #  page["mult_choice_#{choice}"].replace_html "#{choice} (definition: #{answer.definition})"
      #end
    end
  end
  
  #Displaying/picking questions
  def game_entry
    @current_user = current_user
    puts "CURRUSER"
    
    game = Game.find(params[:id].to_i)
    puts @current_user
    user = User.find("guest") #FIXME
    puts user.login
    puts game.gameid
    puts user
    
  	word = Word.find(game.currentword)
    
  	gp = GamePlayer.find(game.gameid, user.login)
  	@score = gp.score
  
    puts "***"
    puts word.word
    
    #Get a random context for the word
    @para = false
    contexts = word.contexts #get context
    con = contexts.sort_by{ rand }.first

  	if(con)
  	  #Initialize paragraph, multiple choice settings
  	  @para_book = con.book;
      @para = con.wordLine
      @para.gsub!(word.word, '___________') #underline the missing word    
      @mc = word.choices 
      @mc_array = (@mc << word.word).shuffle
    else #Find another word to use, no contexts
      wordlist = WordList.find(game.wordlist)
      words = wordlist.words
      
      for word in words
        if !word.contexts.empty?
          game.changeWord(word.wordid)
          redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
          return
        end
      end
    end    
    nexturl = url_for :controller => :games, :action => :game_entry, :id => game.gameid
    @disp = nexturl
  end

  def new_game
    wordlist = WordList.find(params[:wordlist])
    game = Game.createNew(wordlist.name)
    user = User.find(current_user)
    puts user.login
    
    gp = GamePlayer.createNew(game.gameid, user.login)
    
    words = wordlist.words 
    currentword = words[rand(words.length)]
    #currentword = Word.find(1) #FIXME: to above
    game.changeWord(currentword.wordid)
    
    if(currentword) #If there is a word
      #Save the currentword in the session or something?
      redirect_to(:controller=> :games, :action=> :game_entry, :id => game.gameid)
      return
    end
    flash[:notice] = "Wordlist has no words!"
    redirect_to :back
  end
end
