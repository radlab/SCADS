class SearchController < ApplicationController
  def context
    currentword = Word.findWordByWord(params[:word]).first.first
    @contexts = currentword.contexts
    @contexts_size = @contexts.size
    
  end
  
  def search
    

  end
end
