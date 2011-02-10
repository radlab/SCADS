class Game < AvroRecord
  
  #Find word by wordid
  
  def self.all
    game = nil
    return [] #FIXME
  end
  
  def self.createNew(wordlist)
    id = 1
    while self.find(id) != nil
      id = id + 1
    end
    g = Game.new
    g.gameid = id
    g.wordlist = wordlist
    g.currentword = 0
    g.save
    g.save #HACK: call everything twice for piql bug
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
    
    if g and g.user(user) == user #Game exists and it's your game
      return true
    end
    
    return false
  end
  
  def answer
    Word.find(self.currentword)
  end
  
  def changeWord(word)
    self.currentword = word
    self.save
    self.save #HACK: call everything twice for piql bug
  end
  
  def user(user)
    return user #FIXME
    return Game.findGameUser(gameid)
  end
    
end