cd /usr/share/jetty/webapps 
wget -c http://nexus.sonatype.org/downloads/nexus-webapp-1.6.0.war -O /usr/share/jetty/webapps/nexus.war
mkdir -p /home/jetty/nexus
ln -s /home/jetty/nexus /usr/share/jetty/sonatype-work

