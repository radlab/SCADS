class GamePlayer < AvroRecord
  
  def self.createNew(gameid, login)
    gp = GamePlayer.new
    gp.gameid = gameid
    gp.login = login
    gp.score = 0
    gp.save
    gp.save #HACK: call everything twice for piql bug
    gp
  end

  def self.find(gameid, login)
    begin #HACK: rescue exception
      GamePlayer.findGamePlayer(java.lang.Integer.new(gameid), login) #HACK: call everything twice for piql bug
    rescue Exception => e
      puts "exception was thrown"
      puts e
    end
    gp = GamePlayer.findGamePlayer(java.lang.Integer.new(gameid), login) #HACK: call everything twice for piql bug
    puts "***JUST RAN PK QUERY ON GAMEPLAYER***"
    puts gp
    return nil if gp && gp.empty?
    gp = gp.first unless gp == nil || gp.empty?
    gp = gp.first unless gp == nil || gp.empty?
    gp
  end
  
  def incrementScore(amount)
    self.score += amount
    self.save 
    self.save #HACK: call everything twice for piql bug
  end
end
