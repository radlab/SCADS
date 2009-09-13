load.libraries = function() {
	library(quantreg)
	library(sm)
}

create.w.dataset = function(data,latency.col,workload.col) {
	d = data.frame( workload=data[,workload.col], latency=data[,latency.col] )
	d = create.w.features(d)
	
	dataset = list( raw.data=data, meta=list(w.col=workload.col,y.col=latency.col), data=d )
	return(dataset)
}

create.w.features = function(x) {
	w = x[,"workload"]/5000
	x[,paste("w","2",sep="_")] = w^2
	x[,paste("w","3",sep="_")] = w^3 
	x[,paste("w","4",sep="_")] = w^4
	x[,paste("w","5",sep="_")] = w^5
#	x[,paste("exp","w",sep="_")] = exp(w)     #/ 1e2
#	x[,paste("exp","w",2,sep="_")] = exp(w)^2 #/ 1e6
#	x[,paste("exp","w",3,sep="_")] = exp(w)^3 #/ 1e8
#	x[,paste("exp","w",4,sep="_")] = exp(w)^4 #/ 1e10
#	x[,paste("exp","w",5,sep="_")] = exp(w)^5 #/ 1e10
	x[,paste("exp","w",8,sep="_")] = exp(w)^8 #/ 1e20
	return(x)
}


rq.w.fit = function(data) {
	df = data$data
		
	#constraints
	#p = ncol(df)-1
	#R = cbind(0,diag(p))
	#r = rep(0,p)
	
	R = data.frame()
	r = c()
	for (w in seq(0,6000,1000)) {
		wn = w/5000
#		R = rbind(R,c(0,1,2*wn,3*wn^2,8*exp(wn)^8))
		R = rbind(R,c(0, 1, 2*wn, 3*wn^2, 4*wn^3, 5*wn^4, 8*exp(wn)^8))
		r = c(r,0)
	}
	R = rbind(R,c(0,0,0,0,0,0,1)); r=c(r,0) # the exponential always positive
	wn = 7000/5000
#	R = rbind(R,c(0, 1, 2*wn, 3*wn^2, 4*wn^3, 5*wn^4, 8*exp(wn)^8))
#	r = c(r,2)
	
	print(R)
	print(r)
	
	m = rq( latency ~ ., tau=0.5, data=df, R=R, r=r, method="fnc", weights=1/latency^1 )
	return( m )
}

rq.w.predict = function(model,workload) {
	data = create.w.features( data.frame(workload=workload) )
	latency = predict( model, newdata=data )
	return( latency )
}

rq.w.plot = function(model,data) {
	l.max = max( data$raw.data[,data$meta$y.col] )
	w.max = max( data$raw.data[,data$meta$w.col] )
	plot( data$raw.data[,data$meta$w.col], data$raw.data[,data$meta$y.col], bty="n", xlim=c(0,w.max*1.2), ylim=c(0,l.max*1.1),
			xlab="workload", ylab="latency" )
	w = seq(0,w.max*1.2,10)
	l = rq.w.predict(model,w)
	lines( w, l, col="red" )
}


create.gp.dataset = function(data,latency.col,get.w.col,put.w.col) {
	d = data.frame( g=data[,get.w.col], p=data[,put.w.col], latency=data[,latency.col] )
	d = create.gp.features(d)$data
	d = add.noise(d,0.01)
	dataset = list( raw.data=data, meta=list(get.w.col=get.w.col,put.w.col=put.w.col,y.col=latency.col), data=d )
	return(dataset)
}

add.noise = function(x,sd) {
	n = nrow(x)*ncol(x)
	noise = as.data.frame( matrix(1+rnorm(n,0,sd=sd),nrow=nrow(x) ) )
	return( x*noise )
}

create.gp.features = function(x) {
	dp = data.frame(g=rep(0,nrow(x)))
	dg = data.frame(g=rep(0,nrow(x)))
	
	x$g = x[,"g"]/4000; 					dg$g = 1;                      dp$g = 0;
	x$p = x[,"p"]/200;						dg$p = 0;                      dp$p = 1;
	
	g = x$g
	p = x$p
	
	x[,"g2"] = g^2;							dg$g2 = 2*g;                   dp$g2 = 0;
	x[,"g3"] = g^3;							dg$g3 = 3*g^2;                 dp$g3 = 0;
	x[,"g4"] = g^4;							dg$g4 = 4*g^3;                 dp$g4 = 0;
	x[,"expg8"] = exp(g)^8;					dg$expg8 = 8*exp(g)^8;         dp$expg8 = 0;
	x[,"expg15"] = exp(g)^15;				dg$expg15 = 15*exp(g)^15;      dp$expg15 = 0;
	x[,"p2"] = p^2;							dg$p2 = 0;                     dp$p2 = 2*p;
	x[,"p3"] = p^3;							dg$p3 = 0;                     dp$p3 = 3*p^2;
	x[,"p4"] = p^4;							dg$p4 = 0;                     dp$p4 = 4*p^3;
	x[,"expp8"] = exp(p)^8;					dg$expp8 = 0;                  dp$expp8 = 8*exp(p)^8;
	x[,"expp15"] = exp(p)^15;				dg$expp15 = 0;                 dp$expp15 = 15*exp(p)^15;

	x[,"g2_p"] = g^2 * p;					dg$g2_p = 2*g*p;               dp$g2_p = g^2;
	x[,"g3_p"] = g^3 * p;					dg$g3_p = 3*g^2*p;             dp$g3_p = g^3;
	x[,"g4_p"] = g^4 * p;					dg$g4_p = 4*g^3*p;             dp$g4_p = g^4;
	x[,"expg8_p"] = exp(g)^8 * p;			dg$expg8_p = 8*exp(g)^8*p;     dp$expg8_p = exp(g)^8;
	x[,"expg15_p"] = exp(g)^15 * p;			dg$expg15_p = 15*exp(g)^15*p;  dp$expg15_p = exp(g)^15;
	x[,"p2_g"] = p^2 * g;					dg$p2_g = p^2;                 dp$p2_g = 2*p*g;
	x[,"p3_g"] = p^3 * g;					dg$p3_g = p^3;                 dp$p3_g = 3*p^2*g;
	x[,"p4_g"] = p^4 * g;					dg$p4_g = p^4;                 dp$p4_g = 4*p^3*g;
	x[,"expp8_g"] = exp(p)^8 * g;			dg$expp8_g = exp(p)^8;         dp$expp8_g = 8*exp(p)^8*g;
	x[,"expp15_g"] = exp(p)^15 * g;			dg$expp15_g = exp(p)^15;       dp$expp15_g = 15*exp(p)^15*g;

	x[,"g2_p2"] = g^2 * p^2;				dg$g2_p2 = 2*g*p^2;            dp$g2_p2 = g^2*2*p;
	x[,"g3_p2"] = g^3 * p^2;				dg$g3_p2 = 3*g^2*p^2;          dp$g3_p2 = g^3*2*p;
	x[,"g4_p2"] = g^4 * p^2;				dg$g4_p2 = 4*g^3*p^2;          dp$g4_p2 = g^4*2*p;
	x[,"expg8_p2"] = exp(g)^8 * p^2;		dg$expg8_p2 = 8*exp(g)^8*p^2;  dp$expg8_p2 = exp(g)^8*2*p;
	x[,"expg15_p2"] = exp(g)^15 * p^2;		dg$expg15_p2 = 15*exp(g)^15*p^2;dp$expg15_p2 = exp(g)^15*2*p;
	x[,"p3_g2"] = p^3 * g^2;				dg$p3_g2 = p^3*2*g;            dp$p3_g2 = 3*p^2*g^2;
	x[,"p4_g2"] = p^4 * g^2;				dg$p4_g2 = p^4*2*g;            dp$p4_g2 = 4*p^3*g^2;
	x[,"expp8_g2"] = exp(p)^8 * g^2;		dg$expp8_g2 = exp(p)^8*2*g;    dp$expp8_g2 = 8*exp(p)^8*g^2;
	x[,"expp15_g2"] = exp(p)^15 * g^2;		dg$expp15_g2 = exp(p)^15*2*g;  dp$expp15_g2 = 15*exp(p)^15*g^2;
	return( list(data=x, dg=dg, dp=dp) )
}

rq.gp.fit = function(data) {
	df = data$data
		
	#constraints
	#p = ncol(df)-1
	#R = cbind(0,diag(p))
	#r = rep(0,p)

	R = data.frame()
	r = c()
	
#	Nd = 5
#	dd = create.gp.features(data.frame(g=c(1200,1500,1700,2200,3000), p=c(120,100,90,60,20) ))
#	R = cbind(0, rbind(dd$dg,dd$dp) )
#	r = rep(1000,2*Nd)

	# constraint: all exponential features have positive coefficients
	features = colnames(df)[ colnames(df)!="latency" ]
#	for (i in grep("exp",features)) {
#		v = rep(0,length(features)+1)
#		v[i+1] = 1
#		R = rbind(R,v)
#		r = c(r,0)
#		r = c(r,1e-3)
#		r = c(r,1000)
#	}

	abs.bound = 20
	for (i in 1:length(features)) {
		v = rep(0,length(features)+1)
		v[i+1] = 1
		R = rbind(R,v)
		r = c(r,-abs.bound)
		v = rep(0,length(features)+1)
		v[i+1] = -1
		R = rbind(R,v)
		r = c(r,-abs.bound)
	}

#	Nd = 5000
#	dd = create.gp.features(data.frame( g=runif(Nd,0,5000), p=runif(Nd,0,300) ))
#	R1 = cbind(0, rbind(dd$dg,dd$dp) )
#	r1 = rep(1e-10,2*Nd)
#	colnames(R) = colnames(R1)
#	R = rbind(R, R1)
#	r = c(r, r1)

#	print(R)
#	print(r)
	
	m = rq( latency ~ ., tau=0.5, data=df, R=R, r=r, method="fnc", weights=1/latency, model=F )
#	m = rq( latency ~ ., tau=0.5, data=df, R=R, r=r, method="fnc" )
	return( list(model=m,R=R,r=r,meta=data$meta) )
}

rq.gp.predict = function(model,get.w,put.w) {
	data = create.gp.features( data.frame(g=get.w,p=put.w) )$data
	latency = predict( model, newdata=data )
	return( latency )
}

rq.gp.plot = function(model,data) {
	l.max = max( data$raw.data[,data$meta$y.col] )
	w.max = max( data$raw.data[,data$meta$w.col] )
	plot( data$raw.data[,data$meta$w.col], data$raw.data[,data$meta$y.col], bty="n", xlim=c(0,w.max*1.2), ylim=c(0,l.max*1.1),
			xlab="workload", ylab="latency" )
	w = seq(0,w.max*1.2,10)
	l = rq.w.predict(model,w)
	lines( w, l, col="red" )
}

rq.gp.diag = function(model,data,n.split=1) {
	pred = rq.gp.predict(model, data$raw.data[,data$meta$get.w.col], data$raw.data[,data$meta$put.w.col])
	n = nrow(data$data)
	
	for (i in 1:n.split) {
		range = ((i-1)*n/n.split+1):(i*n/n.split)
		plot( pred[range], data$data$latency[range] - pred[range], ylim=c(-200,200), main="pred vs resid" )
		abline(h=0,col="red")
		pause()
	}
	
	raw = data.frame(g=data$raw.data[,data$meta$get.w.col], p=data$raw.data[,data$meta$put.w.col], latency=data$raw.data[,data$meta$y.col])
	for (i in 1:n.split) {
		range = ((i-1)*n/n.split+1):(i*n/n.split)
		slice.data = raw[range,]
		max.get.w = max(slice.data$g)*1.3
		get_workload = seq(0,max.get.w,max.get.w/100)
		put_workload = predict( lm(p~-1+g, data=slice.data), newdata=data.frame(g=get_workload))
		#plot(slice.data$get.w,slice.data$put.w, main="getw vs putw")
		#pause()
		pred = rq.gp.predict(model, get_workload, put_workload)

		plot( slice.data$g+slice.data$p, slice.data$latency, main="latency along slice", type="p", ylim=c(0,300), xlim=c(0,max(slice.data$g+slice.data$p)*1.3) )
		lines( put_workload+get_workload, pred, col="red" )
		pause()
	}

}

rq.gp.diag.2d = function(model,data) {
	rq.gp.diag.2d.raw(model,data,4000,200)
	pause()
	rq.gp.diag.2d.raw(model,data,40000,2000)
}

rq.gp.diag.2d.raw = function(model,data,g.max,p.max) {
	g.range = seq(0,g.max,length=50)
	p.range = seq(0,p.max,length=50)
	g.seq = rep( g.range, length(p.range) )
	p.seq = rep( p.range, each=length(g.range) )
	
	latency = rq.gp.predict(model,g.seq,p.seq)
	plot( c(), xlim=c(min(g.range),max(g.range)), ylim=c(min(p.range),max(p.range)) )
	i = latency<0; 				points( g.seq[i], p.seq[i], col="black", pch=20 )
	i = latency>=0&latency<100; points( g.seq[i], p.seq[i], col="cornflowerblue", pch=20 )
	i = latency>=100;			points( g.seq[i], p.seq[i], col="red", pch=20 )
}

rq.gp.fit.all.types = function(raw.data) {
	models = list()
	models[["get"]] = rq.gp.fit.all.q(raw.data,"get")
	models[["put"]] = rq.gp.fit.all.q(raw.data,"put")
	return(models)
}

rq.gp.fit.all.q = function(raw.data,type) {
	models = list()
	models[["1"]]    = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_01p",sep=""),"get_workload","put_workload") )
	models[["10"]]   = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_10p",sep=""),"get_workload","put_workload") )
	models[["20"]]   = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_20p",sep=""),"get_workload","put_workload") )
	models[["50"]]   = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_50p",sep=""),"get_workload","put_workload") )
	models[["70"]]   = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_70p",sep=""),"get_workload","put_workload") )
	models[["90"]]   = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_90p",sep=""),"get_workload","put_workload") )
	models[["99"]]   = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_99p",sep=""),"get_workload","put_workload") )
	models[["99.9"]] = rq.gp.fit( create.gp.dataset(d,paste(type,"_latency_999p",sep=""),"get_workload","put_workload") )
	return(models)
}

rq.gp.diag.all.q = function(models,raw.data,n.split=1) {
	n = nrow(data$data)
	gc = models[["90"]]$meta$get.w.col
	pc = models[["90"]]$meta$put.w.col
	yc = models[["99"]]$meta$y.col
	raw = data.frame(g=raw.data[,gc], p=raw.data[,pc], latency=raw.data[,yc])
	
	cols = rainbow(length(models))

	for (i in 1:n.split) {
		range = ((i-1)*n/n.split+1):(i*n/n.split)
		slice.data = raw[range,]
		max.get.w = max(slice.data$g)*1.3
		get_workload = seq(0,max.get.w,max.get.w/100)
		put_workload = predict( lm(p~-1+g, data=slice.data), newdata=data.frame(g=get_workload))

		plot( slice.data$g+slice.data$p, slice.data$latency, main="latency along slice", type="p", ylim=c(0,300), xlim=c(0,max(slice.data$g+slice.data$p)*1.3) )
		for (i in 1:length(names(models))) {
			q = names(models)[i]
			pred = rq.gp.predict(models[[q]]$model, get_workload, put_workload)
			lines( put_workload+get_workload, pred, col=cols[i] )
		}
		pause()
	}

}

rq.gp.sample = function(models, type, get.w, put.w, n.samples, latency.threshold=150) {
	cat( paste("rq.gp.sample: type=",type,", get.w=",get.w,", put.w=",put.w,", n.samples=",n.samples,", lat.thr=",latency.threshold,"\n",sep="") )
	if ( rq.gp.overloaded(models, get.w, put.w, latency.threshold) )
		return( rep(Inf,n.samples))
	else if (n.samples==0)
		return( c() )
	else
		return( rq.gp.sample.raw(models[[type]], get.w, put.w, n.samples ))
}

rq.gp.sample.raw = function(models, get.w, put.w, n.samples) {
	q = c()
	qv = c()
	
	for (i in 1:length(models)) {
		qn = names(models)[i]
		lat = rq.gp.predict(models[[qn]]$model, get.w, put.w)
		q = c(q,as.numeric(qn)/100)
		qv = c(qv,lat)
	}
	
	Q = t(matrix(rep(q,n.samples),ncol=n.samples))
	r = runif(n.samples,min(q),max(q))
	rm = t(matrix(rep(r,each=length(q)), ncol=n.samples))
	Q = Q<=rm
	f = apply(Q,1,sum)
	D = data.frame( r=r, q0=q[f], q1=q[f+1], l0=qv[f], l1=qv[f+1] )
	D$s = D$l0 + (D$l1-D$l0)*(D$r-D$q0)/(D$q1-D$q0)
	return(D$s)
}

rq.gp.overloaded = function(models, get.w, put.w, threshold) {
	get.latency = rq.gp.predict( models[["get"]][["99"]]$model, get.w, put.w )
	put.latency = rq.gp.predict( models[["put"]][["99"]]$model, get.w, put.w )
	if (max(c(get.latency,put.latency),na.rm=T)>threshold)
		return(T)
	else
		return(F)
}

dump.models = function(models,file) {
	strings = c()
	for (type in names(models))
		for (q in names(models[[type]]))
			strings = c(strings,model.to.string(models[[type]][[q]],type,q))
	string = paste(strings,collapse="\n")
	cat(string,file=file)
}

model.to.string = function(model,type,q) {
	mc = model$model$coefficients
	paste( type,"-",q,":", paste( c("scale-g","scale-p",names(mc)), c(1/4000,1/200,as.numeric(mc)), sep="=", collapse="," ), sep="" )
}