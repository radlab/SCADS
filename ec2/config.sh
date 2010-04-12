cat <<EOF > /etc/apt/sources.list
###### Ubuntu Main Repos
deb http://us.archive.ubuntu.com/ubuntu/ karmic main restricted universe multiverse 
deb-src http://us.archive.ubuntu.com/ubuntu/ karmic main restricted universe multiverse 

###### Ubuntu Update Repos
deb http://us.archive.ubuntu.com/ubuntu/ karmic-security main restricted universe multiverse 
deb http://us.archive.ubuntu.com/ubuntu/ karmic-updates main restricted universe multiverse 
deb-src http://us.archive.ubuntu.com/ubuntu/ karmic-security main restricted universe multiverse 
deb-src http://us.archive.ubuntu.com/ubuntu/ karmic-updates main restricted universe multiverse 
EOF

apt-get update
yes | apt-get -y install sun-java6-jdk
apt-get -y install jetty

cat <<EOF > /etc/default/jetty
# Defaults for jetty see /etc/init.d/jetty for more

# change to 0 to allow Jetty to start
NO_START=0

# change to 'no' or uncomment to use the default setting in /etc/default/rcS 
VERBOSE=yes

# Run Jetty as this user ID (default: jetty)
# Set this to an empty string to prevent Jetty from starting automatically
JETTY_USER=jetty

# Listen to connections from this network host (leave empty to accept all connections)

#Uncomment to restrict access to localhost
#JETTY_HOST=$(uname -n)

# The network port used by Jetty
JETTY_PORT=8080

# Timeout in seconds for the shutdown of all webapps
#JETTY_SHUTDOWN=30

# Additional arguments to pass to Jetty    
#JETTY_ARGS=

# Extra options to pass to the JVM         
#JAVA_OPTIONS="-Xmx256m -Djava.awt.headless=true"

# Home of Java installation.
#JAVA_HOME=

# The first existing directory is used for JAVA_HOME (if JAVA_HOME is not
# defined in /etc/default/jetty). Should contain a list of space separated directories.
#JDK_DIRS="/usr/lib/jvm/default-java /usr/lib/jvm/java-6-sun"

# Java compiler to use for translating JavaServer Pages (JSPs). You can use all
# compilers that are accepted by Ant's build.compiler property.
#JSP_COMPILER=jikes

# Jetty uses a directory to store temporary files like unpacked webapps
#JETTY_TMP=/var/cache/jetty

# Jetty uses a config file to setup its boot classpath
#JETTY_START_CONFIG=/etc/jetty/start.config

# Default for number of days to keep old log files in /var/log/jetty/
#LOGFILE_DAYS=14
EOF

ln -s /var/lib/jetty/webapps /usr/share/java/webapps
update-rc.d jetty defaults
/etc/init.d/jetty start

apt-get install xinetd

cat <<EOF > /etc/xinetd.d/jetty
service jetty
{
 type = UNLISTED
 disable = no
 socket_type = stream
 protocol = tcp
 user = root
 wait = no
 port = 80
 redirect = 127.0.0.1 8080 
 log_type = FILE /tmp/jettyredirect.log
}
EOF
/etc/init.d/xinetd restart

cd /usr/share/jetty/webapps 
wget -c http://nexus.sonatype.org/downloads/nexus-webapp-1.5.0.war -O /usr/share/jetty/webapps/nexus.war
wget -c http://hudson-ci.org/latest/hudson.war -O /usr/share/jetty/webapps/hudson.war

mkdir -p /home/jetty/hudson
mkdir -p /home/jetty/nexus

chown -R jetty:adm /home/jetty
echo export HUDSON_HOME=/home/jetty/hudson >> /etc/default/jetty
ln -s /home/jetty/nexus /usr/share/jetty/sonatype-work

reboot
