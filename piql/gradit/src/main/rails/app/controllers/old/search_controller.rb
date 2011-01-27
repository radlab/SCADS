class SearchController < ApplicationController
  def context
    @query = params[:word]
    @contexts = Search.search(@query);	
    @contexts_size = @contexts.size	
  end
  
  def search
    
  end
end
