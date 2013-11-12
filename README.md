vsc-zk
======

zookeeper tools
----------------
vsc-zk contains scripts to build zookeeper trees and tools using zookeeper.

zkinitree builds a tree in zookeeper from a config file.
 
vsc-zkrsync uses zookeeper to distribute rsync across multiple nodes 
when rsyncing (large) (shared) filesystems.

A large fraction of rsync calls is spent in gathering the required metadata, 
before performing the actual data synchronisation. This issue becomes worse in the case of an incremental rsync. Distributing the load across multiple processes may lead to a significant performance gain; although other bottlenecks, e.g., access to the 
metadata, might show up.

The implementation uses zookeeper to coordinate the distribution of collecting 
and sync the data across many processes and/or nodes.


Installation of Zookeeper 
--------------------------

A zookeeper server with proper ACLs to a base znode (zookeeper node) is required.

If no such server is available, installation can be performed as follows:

    zkversion=3.4.5
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


### Start zookeeper server
    ./zkServer.sh start $PWD/zk.conf 



Build initial zookeeper tree
-----------------------------
Use zkinitree to build an initial tree in zookeeper from a config file.
It will create paths with specified ACls.

An example config file can be found in examples folder (zkinitree.cfg) 


Usage of zkrsync
-----------------
Example usage for N-parallelised rsync :

Start N+1 sources (first source client will be the Master)
    
    zkrsync -d -S --servers <servers> -u <user> -p <pass> -r <sourcepath> --depth <depth> --session <session> --logfile <logfile>

Start N destinations:
    
    zkrsync -d -D --servers <servers> -u <user> -p <pass> -r <destpath> --depth <depth> --session <session> --logfile <logfile>

Testing pathbuilding: add option --pathsonly

run `zkrsync -H` to see all options

If anything (zookeeper related) goes wrong (no cleanup has been done)

 - kill all running source and destination clients ( of that session)
 - wait like 20 seconds, making sure all zookeeper connections has timed out
 - run exactly ONE Source client, if shutdown was not clean it will recognise it, clean up and exit.
 - If it doesn't clean up but just exists, maybe there is still a client running.
 - you can than start over

Global remarks:

 - Define always a session to make sure you don't mix up different sessions
 - Make sure parameters path, dryrun and delete are all the same: These parameters are not checked (at this moment) but will run with local parameters.
 - Parameter depth is only used on pathbuilding, so the Source Master will always provide this.



