if [ $# -ne 1 ]
then
  echo "[USAGE] $0 [piql.war]"
  exit 1
fi

SERVER=piql.knowsql.org
JETTY_HOME=/usr/share/jetty

echo "[Configuring apt on $SERVER]"
cat scripts/apt.sh | ssh ubuntu@$SERVER "sudo bash"

echo "[Installing Jetty on $SERVER]"
cat scripts/jetty.sh | ssh ubuntu@$SERVER "sudo bash"
cat <<EOF | ssh ubuntu@$SERVER "sudo bash"
/etc/init.d/jetty stop
EOF

echo "[Installing XInetD on $SERVER]"
cat scripts/xinetd.sh | ssh ubuntu@$SERVER "sudo bash"

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
