class SubscriptionsController < ApplicationController
  def create
    @subscription = Subscription.new
    @subscription.owner = current_user.username
    @subscription.target = params[:subscription][:target]
    if @subscription.save
      flash[:notice] = "Subscribed to #{@subscription.target}"
      redirect_to :controller => :users, :action => :show, :id => @subscription.target
    else
      flash[:error] = "Could not subscribe"
      redirect_to current_user
    end
  end

  def destroy
    @subscription.destroy
  end
end