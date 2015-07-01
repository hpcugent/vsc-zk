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
from vsc.zk.base import RunWatchLoopLog
from vsc.zk.rsync.controller import RsyncController

class RunDestination(RunWatchLoopLog):
    """When zookeeperclient is ready, stop"""
    def __init__(self, cmd, **kwargs):

        super(RunDestination, self).__init__(cmd, **kwargs)
        self.registered = False

    def _loop_process_output(self, output):
        """Process the output that is read in blocks
        send it to the logger. The logger need to be stream-like
        Register destination after 2 loops.
        When watch is ready, stop
        """
        super(RunDestination, self)._loop_process_output(output)
        if not self.registered and self._loop_count > 2:
            self.watchclient.add_to_queue()
            self.registered = True


class RsyncDestination(RsyncController):
    """
    Class for controlling rsync with Zookeeper. 
    Starts an rsync daemon available for the RsyncSources. Stops when ready
    """
    BASE_PARTIES = RsyncController.BASE_PARTIES + ['dests']

    def __init__(self, hosts, session=None, name=None, default_acl=None,
                 auth_data=None, rsyncpath=None, rsyncport=None, startport=4444,
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
        self.start_port = startport
        self.port = None

        super(RsyncDestination, self).__init__(**kwargs)

    def get_whoami(self, name=None):  # Override base method
        """Create a unique name for this client"""
        data = [self.daemon_host, str(os.getpid())]
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

    def reserve_port(self):
        """ Search for an available port """
        portdir = '%s/usedports/%s' % (self.session, self.daemon_host)
        self.ensure_path(self.znode_path(portdir))
        lock = self.Lock(self.znode_path(self.session + '/portlock'), self.whoami)
        with lock:
            if self.daemon_port:
                port = self.daemon_port
                portpath = '%s/%s' % (portdir, port)
                if self.exists_znode(portpath):
                    self.log.raiseException('Port already in use: %s' % port)

            else:
                port = self.start_port
                portpath = '%s/%s' % (portdir, port)
                pfree = False
                while (self.exists_znode(portpath)):
                    port += 1
                    portpath = '%s/%s' % (portdir, port)

            self.make_znode(portpath, ephemeral=True)
            portmap = '%s/portmap/%s' % (self.session, self.whoami)
            if not self.exists_znode(portmap):
                self.make_znode(portmap, ephemeral=True, makepath=True)
            self.set_znode(portmap, str(port))

        self.port = port

    def run_rsync(self):
        """ Runs the rsync command """
        config = self.generate_daemon_config()

        cmd = ['rsync', '--daemon', '--no-detach', '--config' , config, '--port', str(self.port)]
        code, output = self.run_with_watch_and_queue(' '.join(cmd))

        os.remove(config)
        return code, output

    def run_netcat(self):
        """ Test run with netcat """
        cmd = ['nc', '-l', '-k', str(self.port)]
        return self.run_with_watch_and_queue(' '.join(cmd))

    def run(self, attempts=3):
        """Starts rsync daemon and add to the queue"""
        self.ready_with_stop_watch()
        attempt = 1
        while (attempt <= attempts and not self.is_ready()):
            self.reserve_port()

            if self.netcat:
                self.run_netcat()
            else:
                self.run_rsync()

            attempt += 1

    def add_to_queue(self):
        """Add this destination to the destination queue """
        self.dest_queue.put('%s:%s' % (self.port, self.whoami))
        self.log.info('Added destination %s to queue with port %s' % (self.whoami, self.port))

    def run_with_watch_and_queue(self, command):
        """ Runs a command that stops when watchclient is ready, also
        registers itself when daemon succesfully running
        """
        code, output = RunDestination.run(command, watchclient=self)
        return code, output
