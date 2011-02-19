class Word < AvroRecord
  
  #Find word by wordid
  
  def self.createNew(id, word, definition, wordlist)
    w = Word.new
    w.wordid = id
    w.word = word
    w.definition = definition
    w.wordlist = wordlist
    w.save
    w
  end

  def self.find(id)
    w = Word.findWord(java.lang.Integer.new(id))
    puts "***JUST RAN PK QUERY ON WORD***"
    puts w
    return nil if w && w.empty?
    w = w.first unless w == nil || w.empty?
    w = w.first unless w == nil || w.empty?
    w
  end
  
  def self.find_by_word(word)
    w = Word.findWordByWord(word)
    if !w.empty?
        return w.first.first
    else
        return nil
    end
  end
  
  #Returns a random word
  def self.randomWords
    #Pick a random number from the words available
     random = rand($NUM_WORDS - 25) #FIXME
     words = []
     for i in (random..random+25)
        words << Word.find(i)
     end
     words.select {|w| w != nil}
  end

=begin
  #Returns an array of 3 other multiple choice options
  def choices
    words = WordList.find(self.wordlist).words
    words = words.shuffle
    words = words.select {|w| w.wordid != self.wordid}
    
    words[0..2].map {|w| w.word}
  end 
=end  
  def contexts
    wc = WordContext.contextsForWord(java.lang.Integer.new(self.wordid)) 
    wc = wc.first unless wc == nil || wc.empty?
    wc
  end

  #contexts method only gives one (first) context?  allContexts gives all contexts
  #each result is given as an array, that is:
  #word.allContexts will give
  #   [<#WordContext> <#WordContext> ] etc.
  
  def getContext
    wc = WordContext.contextsForWord(java.lang.Integer.new(self.wordid)) 
    wc = wc.map { |c| c.first }
    return nil if wc.empty?
    return wc.sort_by {rand}.first
  end
end
