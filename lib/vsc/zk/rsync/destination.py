#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
#
"""
zk.rsync server

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import socket

from kazoo.recipe.queue import LockingQueue
from vsc.zk.rsync.controller import RsyncController

class RsyncDestination(RsyncController):
    """
    Class for controlling rsync with Zookeeper. 
    Starts an rsync daemon available for the RsyncSources. Stops when ready
    """
    BASE_PARTIES = RsyncController.BASE_PARTIES + ['dests']

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, rsyncport=873, netcat=False):  # root default rsyncport: 873

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
            'rsyncpath'   : rsyncpath,
            'netcat'      : netcat,
        }
        super(RsyncDestination, self).__init__(**kwargs)

        host = socket.getfqdn()
        if host.endswith('.os'):  # Temporary fix for vsc
            tmp = host.rsplit('.os', 1)
            host = '.gent.vsc'.join(tmp)
        self.daemon_host = host
        self.daemon_port = rsyncport  # Default

    def get_destss(self):
        """ Get all zookeeper clients in this session registered as destinations"""
        hosts = []
        for host in self.parties['dests']:
            hosts.append(host)
        return hosts

    def daemon_info(self):
        """ Return hostname:port of rsync daemon"""
        return '%s:%s' % (self.daemon_host, str(self.daemon_port))

    def run_rsync(self):
        """ Runs the rsync command """
        # TODO: path in rsync module?
        return self.run_with_watch('rsync --daemon --no-detach --port %s' % self.daemon_port)

    def run_netcat(self):
        """ Test run with netcat """
        return self.run_with_watch('nc -l -k %s' % self.daemon_port)

    def run(self):
        """Starts rsync daemon and add to the queue"""
        self.ready_with_stop_watch()
        # Add myself to dest_queue
        self.dest_queue.put(self.daemon_info())
        if self.netcat:
            self.run_netcat()
        else:
            self.run_rsync()
