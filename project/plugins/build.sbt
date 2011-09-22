resolvers += "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"

//Experimental support for eclipse
resolvers += Classpaths.typesafeResolver

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse" % "1.4.0-RC4")

addSbtPlugin("com.typesafe" % "sbt-ghpages-plugin" % "0.1.0-0.11.0-RC1")

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"

addSbtPlugin("com.github.mpeltonen" %% "sbt-idea" % "0.11.0-SNAPSHOT")