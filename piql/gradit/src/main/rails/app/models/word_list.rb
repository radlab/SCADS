class WordList < AvroRecord
  
  def self.all
    return [self.find("wordlist")]
  end
  
  def self.createNew(name)
    w = WordList.new
    w.name = name
    w.save
    w.save #HACK: call everything twice for piql bug
    w
  end
  
  def self.find(id)
    begin #HACK: rescue exception
      self.findWordList(id) #HACK: call everything twice for piql bug
    rescue Exception => e
      puts "exception was thrown"
      puts e
    end
    wl = self.findWordList(id)
    puts "***JUST RAN PK QUERY ON WORDLIST***"
    puts wl
    return nil if wl && wl.empty?
    wl = wl.first unless wl == nil || wl.empty?
    wl = wl.first unless wl == nil || wl.empty?
    wl
  end
  
  def words
    self.wordsFromWordList(name)
  end
  
end
