class Game < AvroRecord
  
  #Find word by wordid
  
  def self.createNew(wordlist, challenge=0)
    id = 1
    while self.find(id) != nil
      id = id + 1
    end
    g = Game.new
    g.gameid = id
    g.wordlist = wordlist
    
    words = WordList.find(wordlist).words.sort_by{rand}.map {|w| w.wordid }.join(",")
    
    g.words = words
    g.currentword = 0
    g.done = 0
    g.challenge = challenge
    g.save
    
    return g
  end

  def self.find(id)
    g = Game.findGame(java.lang.Integer.new(id))
    puts "***JUST RAN PK QUERY ON GAME***"
    puts g
    return nil if g && g.empty?
    g = g.first unless g == nil || g.empty?
    g = g.first unless g == nil || g.empty?
    g
  end
  
  def self.valid_game?(gameid, user)
    g = Game.find(gameid)
    
    if g and g.users.include? user #Game exists and it's your game
      return true
    end
    
    return false
  end
  
  def answer
    Word.find(self.currentword)
  end
  
  def numWordsLeft
    return self.words.split(",").size
  end
  def hasNextWord
    words_list = self.words.split(",")
    return false if words_list.empty?
    return true
  end
  
  def choices(word)
    words = WordList.find(self.wordlist).words
    words = words.shuffle
    words = words.select {|w| w.word != word}

    words[0..2].map {|w| w.word}
  end


  #Chooses and saves the next word for the game
  def changeWord
    words_list = self.words.split(",")
    wordid = words_list[0].to_i
    words_list = words_list.slice(1..words_list.length - 1)  
    self.words = words_list.join(",")
    self.currentword = wordid
    self.save
    
    return Word.find(wordid)
  end
  
  def users
    return Game.findGameUsers(java.lang.Integer.new(gameid)).map {|u| u.first}.map {|u| u.login}
  end

  def challenge
    return nil if self.challenge == 0  
    challenge =  Game.findChallengesByGame1(self.gameid).concat Game.findChallengesByGame2(self.gameid)
    return challenge.map {|c| c.first}.first
  end
  
  def check_challenge
    challenge = self.challenge
    if challenge != nil
        #Check other one
        if challenge.game1 == self.gameid
            challenge.done = 1 if Game.find(challenge.game2).done == 1
        else
            challenge.done = 1 if Game.find(challenge.game1).done == 1
        end
        challenge.save
    end
    
  end

  def quit
    self.done = 1
    self.save  
    check_challenge
  end
    
end
