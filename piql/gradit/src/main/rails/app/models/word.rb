class Word < AvroRecord
  
  #Find word by wordid
  
  def self.createNew(id, word, definition, wordlist)
    w = Word.new
    w.wordid = id
    w.word = word
    w.definition = definition
    w.wordlist = wordlist
    w.save
    w.save #HACK: call everything twice for piql bug
    w
  end

  def self.find(id)
    begin #HACK: rescue exception
      Word.findWord(java.lang.Integer.new(id)) #HACK: call everything twice for piql bug
    rescue Exception => e
      puts "exception was thrown"
      puts e
    end
    w = Word.findWord(java.lang.Integer.new(id))
    puts "***JUST RAN PK QUERY ON WORD***"
    puts w
    return nil if w && w.empty?
    w = w.first unless w == nil || w.empty?
    w = w.first unless w == nil || w.empty?
    w
  end
  
  def self.find_by_name(word)
    Word.findWordByWord(word).first.first
  end
  
  #Returns a random word
  def self.randomWord
    #Pick a random number from the words available
    random = 1 #FIXME
    Word.findWord(java.lang.Integer.new(random)).first.first
  end
  
  #Returns an array of 3 other multiple choice options
  def choices
    return ["hello", "goodbye", "yay"] #FIXME
    #Randomly pick 3 other words that are not the same as the current word
    #FIXME: 1..20 should be the number of words we have
    selectedWordIds = (1..20).sort_by{rand}[1..4] # first four ids are our four words
    selectedWordIds.delete(self.wordid)
    selectedWords = selectedWordIds.map { |wordid| Word.findWord(java.lang.Integer.new(wordid)).first.first }
    selectedWords.shuffle
    return selectedWords
  end 
  
  def contexts
    begin #HACK: rescue exception
      WordContext.contextsForWord(java.lang.Integer.new(self.wordid))
    rescue Exception => e
      puts "exception was thrown"
      puts e
    end
    wc = WordContext.contextsForWord(java.lang.Integer.new(self.wordid)) #HACK: call everything twice for piql bug
    puts "***JUST CALLED WORD.CONTEXTS***"
    puts wc
    wc = wc.first unless wc == nil || wc.empty?
    wc
  end

end
