
apt-get -y install xinetd

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

