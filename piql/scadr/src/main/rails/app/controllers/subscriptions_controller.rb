class SubscriptionsController < ApplicationController
  def create
    target_username = params[:subscription][:target]
 
    if current_user.nil?
      render :text => "current user doesn't exist"
    elsif User.find(target_username).nil?
      render :text => "target user doesn't exist: #{target_username}"
    else
      @subscription = Subscription.new
      @subscription.owner = current_user.username
      @subscription.target = target_username
      if @subscription.save
        flash[:notice] = "Subscribed to #{@subscription.target}"
        redirect_to :controller => :users, :action => :show, :id => @subscription.target
      else
        flash[:error] = "Could not subscribe"
        redirect_to current_user
      end
    end
  end

  def destroy
    @subscription = Subscription.find(current_user.username, params[:id])
    @subscription.destroy
    flash[:notice] = "Unsubscribed from #{params[:id]}"
    redirect_to user_path(params[:id])
  end
end