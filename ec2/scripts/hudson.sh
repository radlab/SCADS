cd /usr/share/jetty/webapps 
wget -c http://hudson-ci.org/latest/hudson.war -O /usr/share/jetty/webapps/hudson.war
mkdir -p /home/jetty/hudson
echo export HUDSON_HOME=/home/jetty/hudson >> /etc/default/jetty
