class Game < AvroRecord
  
  #Find word by wordid
  
  def self.all
    game = nil
    return ([] << game) if game = Game.find(1)
    return []
  end
  
  def self.createNew(wordlist)
    id = 1
    while self.find(id) != nil
      id = id + 1
    end
    g = Game.new
    g.gameid = id
    g.score = 0
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
  
  def incrementScore(amount)
    self.score += amount
    self.save 
    self.save #HACK: call everything twice for piql bug
  end
  
  def changeWord(word)
    self.currentword = word
    self.save
    self.save #HACK: call everything twice for piql bug
  end
    
end