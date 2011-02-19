class WordList < AvroRecord
  
  def self.all
    return [self.find("wordlist")] #FIXME
  end
  
  def self.createNew(name, user)
    
    return nil if !name or name == "" #Check to make sure fields are there
    #Check if name is already taken
    w = WordList.find(name)
    return nil if w
    
    w = WordList.new
    w.name = name
    w.login = user
    w.save
    w
  end
  
  def self.find(id)
    wl = self.findWordList(id)
    puts "***JUST RAN PK QUERY ON WORDLIST***"
    puts wl
    return nil if wl && wl.empty?
    wl = wl.first unless wl == nil || wl.empty?
    wl = wl.first unless wl == nil || wl.empty?
    wl
  end
  
  def words
    wlw = WordList.wordsFromWordListJoin(self.name).map {|w| w.first}
    #words = wlw.map {|w| Word.find(w.word)}
    return wlw  
  end
  
end
