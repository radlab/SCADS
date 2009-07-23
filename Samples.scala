/**
 * Here is some sample code to use deploylib to the max.
 */
 
/**
 * Most EC2 configuration is now done through environment variables, although
 * the options can also be tweaked in a script.
 * AWS_ACCESS_KEY_ID - required
 * AWS_SECRET_ACCESS_KEY - required
 * AWS_KEY_NAME - required
 *  Can also be modified with DataCenter.keyName
 * AWS_KEY_PATH - required
 *  Can also be modified with DataCenter.keyPath
 * EC2_AMI_32 - required if you ever request a 32-bit instance
 *  Can also be modified with DataCenter.ami32
 * EC2_AMI_64 - required if you ever request a 64-bit instance
 *  Can also be modified with DataCenter.ami64
 * EC2_LOCATION - if not set you are not guaranteed a specific location
 *  Can also be modified with DataCenter.location
 *
 * If you prefer the old way of doing things, where you pass everything in as
 * parameters to the DataCenter.runInstances method, wait for Scala 2.8.
 */
 
/**
 * If you run 'mvn scala:doc' then you will get HTML formatted comments.
 * Just open target/site/scaladocs/scaladocs/index.html
 */
 
/**
 * The parallelMap method provides an easy abstraction to running the same command
 * on several instances in parallel, but what if you want to run different
 * commands on each instance in parallel.
 *
 * The obstacle is that parrallelMap only takes in one function to apply to
 * all the instances. To get around this we can pass in a function that acts
 * differently depending on which instance it is working with.
 */

val x = DataCenter.runInstances(...)
val commands = new HashMap[Instance, String]()

for (i <- 0 until x.size()) commands += (x.get(i), "echo 'I am instance number " + i + "!'")

val responses = x.parallelMap((instance) =>
                              instance.exec(commands(instance)))


/**
 * Requesting instances from EC2 and waiting for them to be ready in one
 * fell swoop.
 *
 * runInstances is an overloaded function that takes can optionally take
 * in a boolean as its last argument. If that argument is true then
 * runInstances won't return until the requested instances are in the ready
 * state.
 */

DataCenter.runInstances(count, instanceType, true)


/**
 * My instances are spread around several different instance groups,
 * but I want to be able to run a method on all of my instances in parallel.
 *
 * You should use the InstanceGroup constructor that takes in a collection of
 * InstanceGroups
 */
 
val xlarges = DataCenter.runInstances(5, "c1.xlarge")
val smalls  = DataCenter.runInstances(6, "m1.small")

new InstanceGroup(Array(xlarges, smalls)).waitUntilReady
 
 
/**
 * With deployNonBlocking you don't have to wait for a deployment to finish.
 * It even works on InstanceGroups in the way that you would expect!
 * Although, the interface to the return value is slightly different
 */

// Starting deployment in parallel:
val appServersDeployment = appServersInstanceGroup.deployNonBlocking(appConfig)
val webServerDeployment = webServerSingleInstance.deployNonBlocking(webConfig)

// Waiting for deployment to finish:
// Notice the difference between whether you called deployNonBlocking on an
// InstanceGroup versus an Instance.
// InstanceGroup's deployNonBlocking returns a method that when called will
// wait on all the instances in the InstanceGroup and return an array of
// responses.
// Instance's deployNonBlocking returns an InstanceThread whose value method
// will wait for the thread to finish and return the response.
appServersDeployment()
webServer.value
 
/**
 * Did you know about InstanceType object?
 * It gives you nice information about instance types, like how many cores
 * it has or how many bits the processor is.
 */
 