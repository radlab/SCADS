class Game < AvroRecord
  
  #Find word by wordid
  
  def self.createNew(wordlist)
    id = 1
    while self.find(id) != nil
      id = id + 1
    end
    g = Game.new
    g.gameid = id
    g.wordlist = wordlist
    
    words = WordList.find(wordlist).words.sort_by{rand}.map {|w| w.wordid }.join(",")
    puts words
    
    g.words = words
    g.currentword = 0
    g.done = 0
    g.save
    g.save #HACK: call everything twice for piql bug
    
    puts "CREATED NEW GAME"
    g
  end

  def self.find(id)
    begin #HACK: rescue exception
      Game.findGame(java.lang.Integer.new(id)) #HACK: call everything twice for piql bug
    rescue Exception => e
      puts "exception was thrown"
      puts e
    end
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
  
  def hasNextWord
    words_list = self.words.split(",")
    return false if words_list.empty?
    return true
  end

  #Chooses and saves the next word for the game
  def changeWord
    puts "CHANGING WORD"
    words_list = self.words.split(",")
    puts words_list.to_s
    wordid = words_list[0].to_i
    puts wordid.to_s
    words_list = words_list.slice(1..words_list.length - 1)  
    puts words_list.to_s
    self.words = words_list.join(",")
    self.currentword = wordid
    self.save
    saved = self.save #HACK: call everything twice for piql bug
    
    return Word.find(wordid)
  end
  
  def users
    return Game.findGameUsers(java.lang.Integer.new(gameid)).map {|u| u.first}.map {|u| u.login}
  end

  def quit
    self.done = 1
    self.save  
  end
    
end
