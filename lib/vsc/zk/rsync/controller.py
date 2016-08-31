# -*- coding: latin-1 -*-
#
# Copyright 2013-2016 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-zk
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
    STATE_PAUSED = 'paused'
    STATE_ACTIVE = 'active'
    STATE_DISABLED = 'disabled'
    STATUS = 'status'

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, netcat=None, verifypath=True, dropcache=False):

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
        }
        self.netcat = netcat

        super(RsyncController, self).__init__(**kwargs)

        self.rsyncpath = rsyncpath.rstrip(os.path.sep)

        if not netcat:
            if verifypath and not self.basepath_ok():
                self.log.raiseException('Path does not exists in filesystem: %s' % rsyncpath)
            if not os.path.isdir(self.RSDIR):
                os.mkdir(self.RSDIR, 0o700)
            self.module = 'zkrs-%s' % self.session

        self.dest_queue = LockingQueue(self, self.znode_path(self.session + '/destQueue'))
        self.verifypath = verifypath
        self.rsync_dropcache = dropcache

    def get_all_hosts(self):
        """Return all zookeeper clients in this rsync session party"""
        hosts = []
        for host in self.parties['allsd']:
            hosts.append(host)
        return hosts

    def basepath_ok(self):
        return os.path.isdir(self.rsyncpath)

    def dest_state(self, dest, state):
        """ Set the destination to a different state """
        destdir = '%s/dests' % self.session
        self.ensure_path(self.znode_path(destdir))
        lock = self.Lock(self.znode_path(self.session + '/destslock'), dest)
        destpath = '%s/%s' % (destdir, dest)
        with lock:
            if not self.exists_znode(destpath):
                self.make_znode(destpath, ephemeral=True)
            current_state, _ = self.get_znode(destpath)
            self.log.debug('Current state is %s, requested state is %s' % (current_state, state))
            if state == self.STATE_PAUSED:
                self.set_paused(destpath, current_state)
            elif state == self.STATE_ACTIVE:
                self.set_active(destpath, current_state)
            elif state == self.STATUS:
                return self.handle_dest_state(dest, destpath, current_state)
            else:
                self.log.error('No valid state: %s ' % state)
                return None
