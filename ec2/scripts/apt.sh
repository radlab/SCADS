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
apt-get -y install lynx wget
yes | apt-get -y install sun-java6-jdk
