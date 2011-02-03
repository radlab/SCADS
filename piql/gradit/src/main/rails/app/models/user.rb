class User < AvroRecord
  
  def self.createNew(login, password, name)
    u = User.new
    u.login = login
    u.password = password
    u.name = name
    u.save
    u.save #HACK: call everything twice for piql bug
    u
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
    
end