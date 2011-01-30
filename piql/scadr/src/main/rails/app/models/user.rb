class User < AvroRecord
  include Comparable
  set_primary_key :username
  # acts_as_authentic do |c|
  #   c.login_field = :username
  # end
 
  # Okay, fuck this.
  # This is way too complicated, and I'd have to reimplement it in AvroRecord...
  #
  # has_many :thoughts, :foreign_key => :owner
  # has_many :subscriptions, :foreign_key => :owner
  # has_many :subscribees, :through => :subscriptions
  # has_many :incoming_subscriptions, :class_name => "Subscription", :foreign_key => :target
  # has_many :subscribers, :through => :incoming_subscriptions

  def to_param
    username
  end

  def following(count)
    Subscription.users_followed_by(username, count)
  end

  def followers(count)
    Subscription.users_following(username, count)
  end

  def my_thoughts(count)
    Thought.my_thoughts(username, count)
  end

  def thoughtstream(count)
    Thought.thoughtstream(username, count)
  end
  
  def <=>(other)
    self.username <=> other.username if other.is_a?(User)
  end
end
