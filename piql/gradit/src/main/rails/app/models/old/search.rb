class Search
  def self.search(query)
	w = Query.wordByWord(query)
  	if(!w.empty?) #If word exists, context might exist
  		w = w.first
  		if(w.contextCacheFromWord($piql_env)) #If word exists in context cache
  			#return w.contexts unless w.context_cache.dirty
  			return w.contextsFromWord(5, $piql_env)
  		end
  	else #Else, create word
  		w = Word.new
  		w.put("word", query)
  		w.save	
  	end
  	
  	#Else, have to search from scratch
    @contexts = Array.new()
    #la = BookLine.find(:all, :conditions => ["line like ?", "%" + query + "%"])
    #REPLACE WITH WORD REFERENCES THINGY
    la = []
    reg = /\b#{query}\b/i
    
    for line in la do
      if (reg.match(line.line))
        beforeline = BookLine.find(:first, :conditions => ["linenum = ? and source = ?", line.linenum - 1, line.source])
        afterline = BookLine.find(:first, :conditions => ["linenum = ? and source = ?", line.linenum + 1, line.source])
        if (beforeline != nil) then beforeline = beforeline.line end
        if (afterline != nil) then afterline = afterline.line end
        bookname = Book.find(line.source).name
        if (line != nil)
        	linecontent = line.line
        	linecontent.gsub!(query, "<b><u>#{query}</u></b>")
    	end
    	
    	#Add contexts in
    	c = Context.new(:before => beforeline, :after => afterline, :wordline => linecontent)
    	c.book = Book.find(line.source)
    	c.word = w;
    	c.save
    	
        @contexts << c
      end
    end
    #Add to context cache
	if(w.context_cache) 
		w.context_cache.dirty = false
	else
		puts "something wrong?"
		cc = ContextCache.new	
		puts cc
		cc.put("dirty", false)
		puts cc
		cc.put("word_word", w)
		puts cc
		cc.save($piql_env)
	end
    return @contexts.sort_by{ rand }
  end
end

