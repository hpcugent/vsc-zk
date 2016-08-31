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
vsc-zk base

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import os
import socket

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, NoAuthError
from kazoo.recipe.party import Party
from vsc.utils import fancylogger
from vsc.utils.run import RunAsyncLoopLog, RunLoopException

RUNRUN_WATCH_EXITCODE = 101
ZKRS_NO_SUCH_SESSION_EXIT_CODE = 14

class RunWatchLoopLog(RunAsyncLoopLog):
    """When zookeeperclient is ready, stop"""
    def __init__(self, cmd, **kwargs):

        self.watchclient = kwargs.pop('watchclient', None)
        super(RunWatchLoopLog, self).__init__(cmd, **kwargs)
        self.log.debug("watchclient %s registered" % self.watchclient)


    def _loop_process_output(self, output):
        """Process the output that is read in blocks
        send it to the logger. The logger need to be stream-like
        When watch is ready, stop
        """
        super(RunWatchLoopLog, self)._loop_process_output(output)
        # self.log.debug("watchclient status %s" % self.watchclient.is_ready())
        if self.watchclient.is_ready():
            self.log.debug("watchclient %s ready" % self.watchclient)
            self.stop_tasks()
            raise RunLoopException(RUNRUN_WATCH_EXITCODE, self._process_output)

run_watch = RunWatchLoopLog.run

class VscKazooClient(KazooClient):

    BASE_ZNODE = '/admin'
    BASE_PARTIES = None

    def __init__(self, hosts, session=None, name=None, default_acl=None, auth_data=None):
        self.log = fancylogger.getLogger(self.__class__.__name__, fname=False)
        self.parties = {}
        self.whoami = self.get_whoami(name)
        self.ready = False

        if session is None:
            session = 'default'
        self.session = session

        kwargs = {
            'hosts'       : ','.join(hosts),
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
          #  'logger'      : self.log
        }
        super(VscKazooClient, self).__init__(**kwargs)
        self.start()
        self.log.debug('Zookeeper client started')

        self.watchpath = self.znode_path(self.session + '/watch')

        if self.BASE_PARTIES:
            self.join_parties(self.BASE_PARTIES)

    def get_whoami(self, name=None):
        """Create a unique name for this client"""
        data = [socket.getfqdn(), str(os.getpid())]
        if name:
            data.append(name)
        res = ':'.join(data)
        self.log.debug("get_whoami: %s" % res)
        return res

    def member_of_party(self, dest, party):
        return dest in self.parties[party]

    def join_parties(self, parties=None):
        """List of parties, join them all"""
        if parties is None or not parties:
            self.log.debug("No parties to join specified")
            parties = []
        else:
            self.log.debug("Joining %s parties: %s", len(parties), ", ".join(parties))
            for party in parties:

                partypath = '%s/%s/parties/%s' % (self.BASE_ZNODE, self.session, party)
                thisparty = Party(self, partypath, self.whoami)
                thisparty.join()
                self.parties[party] = thisparty

    def znode_path(self, znode=None):
        """Create znode path and make sure is subtree of BASE_ZNODE"""
        base_znode_string = self.BASE_ZNODE
        if znode is None:
            znode = base_znode_string
        elif isinstance(znode, (tuple, list)):
            znode = '/'.join(znode)

        if isinstance(znode, basestring):
            if not znode.startswith(base_znode_string):
                if not znode.startswith('/'):
                    znode = '%s/%s' % (self.BASE_ZNODE, znode)
                else:
                    self.log.raiseException('path %s not subpath of %s ' % (znode, base_znode_string))
        else:
            self.log.raiseException('Unsupported znode type %s (znode %s)' % (znode, type(znode)))
        self.log.debug("znode %s" % znode)
        return znode

    def make_znode(self, znode=None, value="", acl=None, **kwargs):
        """Make a znode, raise NodeExistsError if exists"""
        znode_path = self.znode_path(znode)
        self.log.debug("creating znode path: %s" % znode_path)
        try:
            znode = self.create(znode_path, value=value, acl=acl, **kwargs)
        except NodeExistsError:
            self.log.raiseException('znode %s already exists' % znode_path)
        except NoNodeError:
            self.log.raiseException('parent node(s) of znode %s missing' % znode_path)

        self.log.debug("znode %s created in zookeeper" % znode)
        return znode

    def exists_znode(self, znode=None):
        """Checks if znode exists"""
        znode_path = self.znode_path(znode)
        return self.exists(znode_path)

    def set_znode(self, znode=None, value=''):
        znode_path = self.znode_path(znode)
        return self.set(znode_path, str(value))

    def get_znode(self, znode=None):
        znode_path = self.znode_path(znode)
        return self.get(znode_path)

    def znode_acls(self, znode=None, acl=None):
        """set the acl on a znode"""
        znode_path = self.znode_path(znode)
        try:
            self.set_acls(znode_path, acl)
        except NoAuthError:
            self.log.raiseException('No valid authentication for (%s) on path %s!' % (self.auth_data, znode_path))
        except NoNodeError:
            self.log.raiseException('node %s doesn\'t exists' % znode_path)

        self.log.debug("added ACL for znode %s in zookeeper" % znode_path)

    def set_watch_value(self, watch, value):
        """Sets the value of an existing watch"""
        watchpath = '%s/%s' % (self.watchpath, watch)
        self.set(watchpath, value)

    def new_watch(self, watch, value):
        """ Start a watch other clients can register to """
        watchpath = '%s/%s' % (self.watchpath, watch)
        if self.exists(watchpath):
            self.log.error('watchnode %s already exists!' % watchpath)
            return False
        return self.make_znode(watchpath, value, makepath=True)

    def remove_watch(self, watch):
        """ Removes a wath """
        watchpath = '%s/%s' % (self.watchpath, watch)
        self.delete(watchpath)

    def set_ready(self):
        """Set when work is done"""
        self.ready = True

    def is_ready(self):
        return self.ready

    def start_ready_watch(self):
        """ Start a ready watch other clients can register to """
        return self.new_watch('ready', 'start')

    def stop_ready_watch(self):
        """ Stops the ready watch"""
        self.set_watch_value('ready', 'stop')

    def remove_ready_watch(self):
        """ removes the ready watch"""
        self.remove_watch('ready')


    def exit(self):
        """stop and close the connection"""
        self.stop()
        self.close()

    def ready_with_stop_watch(self):
        """Register to watch and set to ready when watch is set to 'stop' """
        watchpath = '%s/%s' % (self.watchpath, 'ready')
        @self.DataWatch(watchpath)
        # pylint: disable=unused-variable,unused-argument
        def ready_watcher(data, stat):
            self.log.debug("Watch status is %s" % data)
            if data == 'stop':
                self.log.debug('End node received, set ready')
                self.set_ready()

    def run_with_watch(self, command):
        """ Runs a command that stops when watchclient is ready """
        code, output = run_watch(command, watchclient=self)
        return code, output
