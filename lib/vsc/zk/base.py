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
vsc-zk base

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

import os
import socket

from kazoo.client import KazooClient
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from kazoo.exceptions import NodeExistsError, NoNodeError, NoAuthError


class VscKazooClient(KazooClient):

    BASE_ZNODE = '/admin'
    BASE_PARTIES = None

    def __init__(self, hosts, name=None, default_acl=None, auth_data=None):
        self.log = fancylogger.getLogger(self.__class__.__name__, fname=False)
        self.parties = {}
        self.whoami = self.get_whoami(name)

        kwargs={
        'hosts'       : ','.join(hosts),
        'default_acl' : default_acl,
        'auth_data'   : auth_data,
        'logger'      : self.log
        }
        super(VscKazooClient, self).__init__(**kwargs)
        self.start()
        self.log.debug('started')
        if self.BASE_PARTIES:
            self.join_parties(self.BASE_PARTIES)

    def get_whoami(self, name=None):
        """Create a unique name for this client"""
        data = [socket.gethostname(), os.getpid()]
        if name:
            data.append(name)

        res = '-'.join(str(x) for x in data)
        self.log.debug("get_whoami: %s" % res)
        return res

    def join_parties(self, parties=None):
        """List of parties, join them all"""
        if parties is None or not parties:
            self.log.debug("No parties to join specified")
            parties = []
        else:
            self.log.debug("Joining %s parties: %s", len(parties), ", ".join(parties))

        for party in parties:
            self.partiesx

    def znode_path(self, znode=None):
        """Create znode path and make sure is subtree of BASE_ZNODE"""
        base_znode_string = os.path.join(self.BASE_ZNODE)
        if znode is None:
            znode = base_znode_string
        elif isinstance(znode, (tuple, list)):
            znode = os.path.join(znode)
        
        if isinstance(znode, basestring):
            if not znode.startswith(base_znode_string):
                if not znode.startswith('/'):
                    znode = os.path.join(self.BASE_ZNODE, znode)
                else:
                    self.log.raiseException('path %s not subpath of %s ' % (znode, base_znode_string))
        else:
            self.log.raiseException('Unsupported znode type %s (znode %s)' % (znode, type(znode)))
        
        return znode

    def make_znode(self, znode=None,value="",acl=None, **kwargs ):
        """Make a znode, raise exception if exists"""
        znode_path = self.znode_path(znode)
        self.log.debug("znode path is: %s" % znode_path)
        try:
            znode = self.create(znode_path,value=value,acl=acl,**kwargs)
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
        
    def znode_acls(self,znode=None,acl=None):
        """set the acl on a znode"""
        znode_path = self.znode_path(znode)
        try:
            self.set_acls(znode_path, acl)
        except NoAuthError:
            self.log.raiseException('No valid authentication!')
        except NoNodeError:  
            self.log.raiseException('node %s doesn\'t exists' % znode_path)
        
        self.log.debug("added ACL for znode %s in zookeeper" % znode_path)
       