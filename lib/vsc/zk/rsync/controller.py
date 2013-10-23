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
zk.rsync controller

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""
import sys, os

from kazoo.recipe.queue import LockingQueue
from vsc.zk.base import VscKazooClient

class RsyncController(VscKazooClient):
    """
    Class for controlling Rsync with Zookeeper. 
    Use the child classes RsyncSource and RsyncDestination.
    """

    BASE_ZNODE = '/admin/rsync'
    BASE_PARTIES = ['allsd']

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, netcat=None):

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
        }
        self.netcat = netcat

        super(RsyncController, self).__init__(**kwargs)

        if not netcat and not os.path.isdir(rsyncpath):
            self.log.raiseException('Path does not exists in filesystem: %s' % rsyncpath)
        else:
            self.rsyncpath = rsyncpath

        self.dest_queue = LockingQueue(self, self.znode_path(self.session + '/destQueue'))

    def get_all_hosts(self):
        """Return all zookeeper clients in this rsync session party"""
        hosts = []
        for host in self.parties['allsd']:
            hosts.append(host)
        return hosts
