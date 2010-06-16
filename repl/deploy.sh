#!/bin/bash

mvn clean package -DskipTests
rsync target/repl-1.0-SNAPSHOT.war ubuntu@184.72.54.79:/usr/share/jetty/webapps/root.war
ssh ubuntu@piql.knowsql.org "sudo /etc/init.d/jetty stop"
ssh ubuntu@piql.knowsql.org "sudo /etc/init.d/jetty start"
