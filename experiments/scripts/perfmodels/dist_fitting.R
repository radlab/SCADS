load.libraries = function() {
	library(e1071)
}



fit.gamma = function(data) {
	d.mean = mean(data,na.rm=T)
	d.var = var(data,na.rm=T)
	d.skew = skewness(data,na.rm=T)
	
	shape = 4/(d.skew*d.skew)
	scale = sqrt(d.var/shape)
	loc = d.mean - shape*scale
	
	return( list(shape=shape,scale=scale,loc=loc) )
}

plot.gamma = function(params,xrange,new=T,NS=1000) {
	y = dgamma( xrange, shape=params$shape, scale=params$scale )
	if (new) plot(xrange + params$loc,y,col="red", type="l")
	else lines(xrange + params$loc,y,col="red")
}