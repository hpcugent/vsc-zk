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
zk.rsync destination

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import ConfigParser
import os
import socket
import tempfile

from kazoo.recipe.queue import LockingQueue
from vsc.zk.rsync.controller import RsyncController

class RsyncDestination(RsyncController):
    """
    Class for controlling rsync with Zookeeper. 
    Starts an rsync daemon available for the RsyncSources. Stops when ready
    """
    BASE_PARTIES = RsyncController.BASE_PARTIES + ['dests']

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, rsyncport=873,  # root default rsyncport: 873
                 netcat=False, domain=None):

        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
            'rsyncpath'   : rsyncpath,
            'netcat'      : netcat,
        }

        host = socket.getfqdn()
        if domain:
            hname = host.split('.', 1)
            host = '%s.%s' % (hname[0], domain)
        self.daemon_host = host
        self.daemon_port = rsyncport

        super(RsyncDestination, self).__init__(**kwargs)

    def get_whoami(self, name=None):  # Override base method
        """Create a unique name for this client"""
        data = [self.daemon_host, str(self.daemon_port), str(os.getpid())]
        if name:
            data.append(name)

        res = ':'.join(data)
        self.log.debug("get_whoami: %s" % res)
        return res

    def get_destss(self):
        """ Get all zookeeper clients in this session registered as destinations"""
        hosts = []
        for host in self.parties['dests']:
            hosts.append(host)
        return hosts

    def daemon_info(self):
        """ Return hostname:port of rsync daemon"""
        return '%s:%s' % (self.daemon_host, str(self.daemon_port))

    def generate_daemon_config(self):
        """ Write config file for this session """
        fd, name = tempfile.mkstemp(dir=self.RSDIR, text=True)
        file = os.fdopen(fd, "w")
        config = ConfigParser.RawConfigParser()
        config.add_section(self.module)
        config.set(self.module, 'path', self.rsyncpath)
        config.set(self.module, 'read only', 'no')
        config.set(self.module, 'uid', 'root')
        config.set(self.module, 'gid', 'root')
        config.write(file)
        return name

    def run_rsync(self):
        """ Runs the rsync command """
        config = self.generate_daemon_config()
        code, output = self.run_with_watch('rsync --daemon --no-detach --config=%s --port %s'
                                   % (config, self.daemon_port))
        os.remove(config)
        return code, output

    def run_netcat(self):
        """ Test run with netcat """
        return self.run_with_watch('nc -l -k %s' % self.daemon_port)

    def run(self):
        """Starts rsync daemon and add to the queue"""
        self.ready_with_stop_watch()
        # Add myself to dest_queue
        self.dest_queue.put(self.whoami)
        if self.netcat:
            self.run_netcat()
        else:
            self.run_rsync()
