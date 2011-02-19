class WordContext < AvroRecord
  
  def self.createNew(word, book, linenum, wordline)
    wc = WordContext.new
    wc.word = word
    wc.book = book
    wc.linenum = linenum
    wc.wordLine = wordline
    wc.save
    wc
  end
end
