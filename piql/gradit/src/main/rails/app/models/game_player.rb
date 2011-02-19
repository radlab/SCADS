class GamePlayer < AvroRecord
  
  def self.createNew(gameid, login)
    gp = GamePlayer.new
    gp.gameid = gameid
    gp.login = login
    gp.score = 0
    gp.save
    gp
  end

  def self.find(gameid, login)
    gp = GamePlayer.findGamePlayer(java.lang.Integer.new(gameid), login) 
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
  end
end
