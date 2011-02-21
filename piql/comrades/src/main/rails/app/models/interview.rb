class Interview < AvroRecord
  def self.human_name
    "Interview"
  end

  def self.find(email, created_at)
    interview = Interview.find_interview(email, created_at)
    interview.present? ? interview.first.first : nil
  end
  
  def to_param
    created_at.to_s
  end
end