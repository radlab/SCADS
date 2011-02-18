class Subscription < AvroRecord
  set_primary_keys :owner, :target

  def self.find(owner, target)
    raw_subs = Subscription.find_subscription(owner, target)
    raw_subs.present? ? @subscription = raw_subs.first.first : @subscription = nil
  end

  def to_param
    target
  end
  
  # Force approved until we know what to do with it...
  def save
    self.approved = true
    super
  end
end