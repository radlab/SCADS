class WordListWord < AvroRecord
  
  def self.createNew(word, wordlist)
    wlw = WordListWord.new
    wlw.word = word
    wlw.wordlist = wordlist
    wlw.save
  end
end
