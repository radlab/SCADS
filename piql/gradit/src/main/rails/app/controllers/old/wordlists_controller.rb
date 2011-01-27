class WordlistsController < ApplicationController
  # GET /wordlists
  # GET /wordlists.xml
  def index
    @wordlists = Wordlist.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @wordlists }
    end
  end

  # GET /wordlists/1
  # GET /wordlists/1.xml
  def show
    @wordlist = Query.wordlistByName(params[:name])
    puts @wordlist
    @wordlist = @wordlist.first

    respond_to do |format|
      format.html # show.html.erb
      format.xml  { render :xml => @wordlist }
    end
  end

  # GET /wordlists/new
  # GET /wordlists/new.xml
  def new
    @wordlist = Wordlist.new

    respond_to do |format|
      format.html # new.html.erb
      format.xml  { render :xml => @wordlist }
    end
  end

  # GET /wordlists/1/edit
  def edit
    @wordlist = Wordlist.find(params[:id])
  end

  # POST /wordlists
  # POST /wordlists.xml
  def create
    @wordlist = Wordlist.new(params[:wordlist])

    respond_to do |format|
      if @wordlist.save
        flash[:notice] = 'Wordlist was successfully created.'
        format.html { redirect_to(@wordlist) }
        format.xml  { render :xml => @wordlist, :status => :created, :location => @wordlist }
      else
        format.html { render :action => "new" }
        format.xml  { render :xml => @wordlist.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /wordlists/1
  # PUT /wordlists/1.xml
  def update
    @wordlist = Wordlist.find(params[:id])

    respond_to do |format|
      if @wordlist.update_attributes(params[:wordlist])
        flash[:notice] = 'Wordlist was successfully updated.'
        format.html { redirect_to(@wordlist) }
        format.xml  { head :ok }
      else
        format.html { render :action => "edit" }
        format.xml  { render :xml => @wordlist.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /wordlists/1
  # DELETE /wordlists/1.xml
  def destroy
    @wordlist = Wordlist.find(params[:id])
    @wordlist.destroy

    respond_to do |format|
      format.html { redirect_to(wordlists_url) }
      format.xml  { head :ok }
    end
  end
end
