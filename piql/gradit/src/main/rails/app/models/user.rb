class User < AvroRecord
  require 'digest/md5'
  
  def games
    games = User.findGamesByUser(login)
    return games if games.empty?
    games = games.map {|g| g[1]} 
    return games
  end
  
  def wordlists 
    wordlists = User.findWordListsByUser(login) 
    return wordlists if wordlists.empty?
    return wordlists.map {|w| w.first}
  end
  
  def self.createNew(login, password, name)
    return nil if !login or !password or !name or login == "" or password == "" or name == ""
    
    #Check if username is already taken
    u = User.find(login)
    return nil if u
    
    u = User.new
    u.login = login
    u.password = Digest::MD5.hexdigest(password);
    u.name = name
    u.save
    return u

  end

  def self.find(login)
    u = User.findUser(login) 
    puts "***JUST RAN PK QUERY ON USER***"
    puts u
    return nil if u && u.empty?
    u = u.first unless u == nil || u.empty?
    u = u.first unless u == nil || u.empty?
    u
  end
  
  def self.guest_user
    u = User.find("guest")
    return u if u
    return User.createNew("guest", "password", "Guest User")
  end
  
  def self.login(login, password)
    u = User.find(login)
    
    if u 
      puts u.password
      puts Digest::MD5.hexdigest(password)
    end
    
    if u and u.password == Digest::MD5.hexdigest(password) #Successful login
      return u
    end
    return nil
  end
   
  def self.get_leaderboard
    return User.leaderboard.map {|gp| gp.first}
  end
    
end
