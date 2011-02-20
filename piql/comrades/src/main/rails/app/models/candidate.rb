class Candidate < AvroRecord
  def to_param
    email
  end

  def self.find(email)
    candidate = Candidate.find_candidate(email)
    candidate.present? ? candidate.first.first : nil
  end
  
  def self.waiting(field)
    candidates = Candidate.find_waiting(field)
    candidates.present? ? candidates.collect{ |c| c.first } : []
  end

  def self.top_rated(field)
    candidates = Candidate.find_top_rated(field)
    candidates.present? ? candidates.collect{ |c| c.first } : []
  end
end