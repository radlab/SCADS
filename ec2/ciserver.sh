cat scripts/apt.sh | ssh ubuntu@scads.knowsql.org "sudo bash"
cat scripts/jetty.sh | ssh ubuntu@scads.knowsql.org "sudo bash"
cat scripts/nexus.sh | ssh ubuntu@scads.knowsql.org "sudo bash"
cat scripts/hudson.sh | ssh ubuntu@scads.knowsql.org "sudo bash"
cat scripts/xinetd.sh | ssh ubuntu@scads.knowsql.org "sudo bash"

#Apply nexus configuration... this is gross, i know...
ssh ubuntu@scads.knowsql.org "sudo chown -R ubuntu /home/jetty"
scp -r conf/nexus ubuntu@scads.knowsql.org:/home/jetty/nexus/conf
ssh ubuntu@scads.knowsql.org "sudo chown -R jetty /home/jetty"
ssh ubuntu@scads.knowsql.org "sudo reboot"
