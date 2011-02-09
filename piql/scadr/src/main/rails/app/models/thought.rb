class Thought < AvroRecord
  include Comparable
  # NOTE:
  # This is what thoughtstream actually returns:
  # [Subscription, Thought]

  def to_param
    timestamp.to_s
  end

  def posted_at
    post_time = Time.at(timestamp)
    if Time.now - post_time < 1.hour
      "#{((Time.now - post_time)/60).to_i} minutes ago"
    elsif Time.now - post_time < 1.day
      "#{((Time.now - post_time)/60/60).to_i} hours ago"
    else
      post_time.strftime("on %A, %B %d")
    end
  end
  
  def <=>(other)
    self.timestamp <=> other.timestamp if other.is_a?(Thought)
  end
end
