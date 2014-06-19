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
# http://github.com/hpcugent/vsc-zk
#
# vsc-zk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-zk is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-zk. If not, see <http://www.gnu.org/licenses/>.
#
"""
zk.rsync controller

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import os
import sys

from kazoo.recipe.queue import LockingQueue
from vsc.zk.base import VscKazooClient

class RsyncController(VscKazooClient):
    """
    Class for controlling Rsync with Zookeeper. 
    Use the child classes RsyncSource and RsyncDestination.
    """

    BASE_ZNODE = '/admin/rsync'
    BASE_PARTIES = ['allsd']
    RSDIR = '/tmp/zkrsync'

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

        if not netcat:
            rsyncpath = rsyncpath.rstrip(os.path.sep)
            if not os.path.isdir(rsyncpath):
                self.log.raiseException('Path does not exists in filesystem: %s' % rsyncpath)
            if not os.path.isdir(self.RSDIR):
                os.mkdir(self.RSDIR, 0700)
            self.module = 'zkrs-%s' % self.session

        self.rsyncpath = rsyncpath
        self.dest_queue = LockingQueue(self, self.znode_path(self.session + '/destQueue'))

    def get_all_hosts(self):
        """Return all zookeeper clients in this rsync session party"""
        hosts = []
        for host in self.parties['allsd']:
            hosts.append(host)
        return hosts
