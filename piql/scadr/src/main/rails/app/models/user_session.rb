class UserSession
  attr_accessor :username

  def initialize(params={})
    @username = params[:username]
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
    not user.nil?
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