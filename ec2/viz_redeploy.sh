# Used when server is running but want to deploy new jar

if [ $# -ne 1 ]
then
  echo "[USAGE] $0 [scadr.war]"
  exit 1
fi

SERVER=piql.knowsql.org
JETTY_HOME=/usr/share/jetty

echo "[Stopping Jetty]"
cat <<EOF | ssh ubuntu@$SERVER "sudo bash"
/etc/init.d/jetty stop
EOF

echo "[Deploying $1 to root context]" 
scp $1 ubuntu@$SERVER:~/root.war
cat <<EOF | ssh ubuntu@$SERVER "sudo bash" 
rm -rf $JETTY_HOME/webapps/root
mv /home/ubuntu/root.war $JETTY_HOME/webapps
EOF

echo "[Restarting Jetty. Should be good to go now]" 
cat <<EOF | ssh ubuntu@$SERVER "sudo bash"
/etc/init.d/jetty start
EOF
