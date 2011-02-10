class User < AvroRecord
  require 'digest/md5'
  
  def games
    return [] #FIXME
    return User.findGamesByUser(login)
  end
  
  def wordlists 
    return [] #FIXME
    return User.findWordListsByUser(login)
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
    u.save #HACK: call everything twice for piql bug
    return u

  end

  def self.find(login)
    begin #HACK: rescue exception
      User.findUser(login) #HACK: call everything twice for piql bug
    rescue Exception => e
      puts "exception was thrown"
      puts e
    end
    u = User.findUser(login) #HACK: call everything twice for piql bug
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
    puts "**USER LOGGING IN**"
    puts u
    
    if u 
      puts u.password
      puts Digest::MD5.hexdigest(password)
    end
    
    if u and u.password == Digest::MD5.hexdigest(password) #Successful login
      return u
    end
    return nil
  end
    
end