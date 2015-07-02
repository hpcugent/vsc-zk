vsc-zkrsync uses zookeeper to distribute rsync across multiple nodes 
when rsync ing (large) (shared) filesystem.

Large fraction of rsync calls is spend in agthering the required metadata, 
and then performing the actual data synchronisation. Especially when dealing 
with "incremental" rsync. Spreading the load over multiple processes can mean 
a significant speed increase; although other bottlenecks, eg access to the 
metadata, might show up.

The implementation uses zookeeper to coordinate the distribution of collecting 
and sync the data across many processes and/or nodes.

= Zookeeper =
== Install zookeeper-server ==
A zookeeper server with proper ACLs to a base znode is required.

If none is available, installation can be performed as follows:

zkversion=3.4.6
basepath=/tmp/$USER # is not permanent
chmod 700 $basepath

mkdir -p $basepath/zk/server/data

cd $basepath/zk/server
# use another mirror if needed
wget http://apache.belnet.be/zookeeper/zookeeper-$zkversion/zookeeper-$zkversion.tar.gz
tar xzf zookeeper-$zkversion.tar.gz

rm -f zkServer.sh
cat > zkServer.sh <<EOF
#!/bin/bash
./zookeeper-$zkversion/bin/zkServer.sh \$@
EOF
chmod +x zkServer.sh


cat > zk.conf <<EOF
tickTime=2000
dataDir=$PWD/data
clientPort=2181
EOF


== start zookeeper-server ==
./zkServer.sh start $PWD/zk.conf 

