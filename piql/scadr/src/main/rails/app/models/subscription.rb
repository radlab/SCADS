class Subscription < AvroRecord
  set_primary_keys :owner, :target

  def to_param
    target
  end
  
  # Force approved until we know what to do with it...
  def save
    self.approved = true
    super
  end
end