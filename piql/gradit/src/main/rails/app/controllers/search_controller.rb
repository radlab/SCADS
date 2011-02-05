class SearchController < ApplicationController
  def context
    puts "myword"
    currentword = Word.find_by_word(params[:word])
    if currentword != nil
        @contexts = currentword.allContexts
        @contexts_size = @contexts.size
    else
        @contexts = nil
        @contexts_size = 0
    end

    
  end
  
  def search
    

  end
end
