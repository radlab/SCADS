var le: LoadExp = null
var re: ReadExp = null


(1 to 5).foreach(i =>{
  val inst = (1 to 5).toList.map(j => EC2.runInstance)
  inst.foreach(_.hostname)

	le = new LoadExp(EC2.myInstances, 5, true, i * 1000000)
	le.postTestCollection()

	(1 to 5).foreach(j => {
		re = new ReadExp(EC2.myInstances, EC2.myInstances, 35)
		re.loadServices.foreach(_.blockTillDown)
	})
})
