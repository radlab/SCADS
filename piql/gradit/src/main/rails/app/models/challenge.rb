class Challenge < AvroRecord
  
  def self.createNew(user1, user2, game1, game2)
    c = Challenge.new
    
    c.timestamp = Time.now.to_s
    c.user1 = user1
    c.user2 = user2
    c.game1 = game1
    c.game2 = game2
    c.score1 = 0
    c.score2 = 0
    c.done = 0
    c.save
    c
  end
end
