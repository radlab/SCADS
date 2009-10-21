library(sm)

history = function() {

	actions = read.csv("~/Downloads/scads/transients/lowlevelactions1.csv")
	effects = read.csv("~/Downloads/scads/transients/actioneffects1.csv")
	head(data)
	unique( data$action )
	
	plot.duration(actions,"copy")
	plot.duration(actions,"removeData")
	plot.duration(actions,"removeData_remove")
	plot.duration(actions,"removeData_add")
	plot.duration(actions,"remove")
	
	plot.duration(actions,"getNodeRange")
	
	changes = plot.effects(actions,effects)
	
	fit.effects(changes)
}

fit.effects = function(changes) {
	
	for (action in c("copy","remove","removeData")) {
		i = changes$action==action

		model50 = lm( lat50_transient ~ -1 + lat50_steady, data = changes[i,])
		model99 = lm( lat99_transient ~ -1 + lat99_steady, data = changes[i,])

		print(model50)
		print(model99)

		plot( changes$lat50_steady[i], changes$lat50_transient[i], main=paste("change in 50p for ",action,sep="") ) #, xlim=c(0,100), ylim=c(0,100) )
		abline(0, model50$coefficients[1], col="blue", lwd=2)
		abline(0, 1, lty=2, lwd=1)
		pause()

		plot( changes$lat99_steady[i], changes$lat99_transient[i], main=paste("change in 99p for ",action,sep="") ) #, xlim=c(0,1000), ylim=c(0,1000) )
		abline(0, model99$coefficients[1], col="blue", lwd=2)
		abline(0, 1, lty=2, lwd=1)
		pause()
	}
	
}

plot.effects = function(actions,effects) {
	
	actions = actions[ actions$action=="copy"|actions$action=="removeData"|actions$action=="remove", ]
	#actions = actions[ actions$action=="remove", ]
	#actions = actions[ actions$action=="copy", ]
	#actions = actions[ actions$action=="removeData", ]
	latency.change = data.frame()		
	
	plot.effects = F
	
	for (id in actions$actionid) {
		ai = actions$actionid==id
		t0 = actions[ai,"time"]
		t1 = actions[ai,"time"] + actions[ai,"duration"]
		name = actions[ai,"action"]
		#print( actions[ai,] )
		
		es = effects[ effects$actionid==id, ]
		for (server in unique(es$server)) {
			e = es[ es$server==server, ]

			x0 = min( min(e$time), t0-60*1000 )
			x1 = max( max(e$time), t0+60*1000 )

			if (plot.effects) {
				layout( matrix(1:3,byrow=T) )
				plot( e$time, e$workload, xlim=c(x0,x1), main=paste(name," @ ",server,sep="") )
				lines( c(t0,t1), c(mean(e$workload),mean(e$workload)), col="red", lwd=10 )
				plot( e$time, e$latency_50p, xlim=c(x0,x1) )
				lines( c(t0,t1), c(mean(e$latency_50p),mean(e$latency_50p)), col="red", lwd=10 )
				plot( e$time, e$latency_99p, xlim=c(x0,x1) )
				lines( c(t0,t1), c(mean(e$latency_99p),mean(e$latency_99p)), col="red", lwd=10 )
				pause()
			}
			
			et0 = e$time<t0 | e$time>t1
			et1 = e$time>=t0 & e$time<=t1
			
			latency.change = rbind( latency.change, data.frame(action=name, lat50_steady=mean( e[et0,"latency_50p"] ), lat50_transient=mean( e[et1,"latency_50p"] ), lat99_steady=mean( e[et0,"latency_99p"] ), lat99_transient=mean( e[et1,"latency_99p"] ) ))
			
		}
	}
	
	latency.change
	
}

plot.duration = function(data,action) {
	
	d = data[ data$action==action, ]
	
	if ( sum( is.nan(d$nkeys) ) > 0 ) {
		hist( d$duration, n=30, col="gray", main=paste("duration of ",action,sep=""), xlab="duration [ms]" )
		print( paste("mean = ", mean(d$duration), sep=""))
	} else {
		plot( d$nkeys/1000, d$duration/1000, xlab="# keys [x 1000]", ylab="duration [sec]", bty="n", main=paste("duration of ",action,sep=""))
#		plot( d$nkeys, d$duration, xlab="# keys [x 1000]", ylab="duration [sec]", bty="n", main=paste("duration of ",action,sep=""))
		model = lm( duration ~ -1 + nkeys, data = d)
		print(model)
		#abline(model$coefficients[1]/1000, model$coefficients[2], col="blue", lwd=2)
		abline(0, model$coefficients[1], col="blue", lwd=2)
	}
}