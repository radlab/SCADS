class UserSession
  attr_accessor :username
  attr_accessor :password

  def initialize(params={})
    @username = params[:username]
    @password = params[:password]
  end

  def new_record?
    true
  end

  def self.find(username)
    session = UserSession.new :username => username
    return nil if session.user.nil?
    session
  end

  def valid?
    !user.nil? && !password.nil? && user.password == Digest::MD5.hexdigest(password)
  end

  def save
    valid?
  end

  def user
    return nil if username.nil?
    results = User.find_user(username)
    results.present? ? results.first.first : nil
  end

  def destroy
    true
  end

end