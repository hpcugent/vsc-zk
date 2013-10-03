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
"""

import os
import socket

import kazoo.client
from vsc.utils import fancylogger


class KazooClient(kazoo.client.KazooClient):

    BASE_ZNODE = ['/admin']
    BASE_PARTIES = None

    def __init__(self, hosts, name=None):
        self.log = fancylogger.getLogger(self.__class__.__name__, fname=False)
        self.parties = {}
        self.whoami = self.get_whoami(name)

        super(KazooClient, self).__init__(hosts=','.join(hosts))
        self.start()

        if self.BASE_PARTIES:
            self.join_parties(self.BASE_PARTIES)

    def get_whoami(self, name=None):
        """Create a unique name for this client"""
        data = [socket.gethostname(), os.getpid()]
        if name:
            data.append(name)

        res = '-'.join(data)
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
        base_znode_string = os.path.join(*self.BASE_ZNODE)
        if znode is None:
            znode = base_znode_string
        elif isinstance(znode, (tuple, list)):
            znode = os.path.join(*znode)

        if isinstance(znode, basestring):
            if not znode.startswith(base_znode_string):
                znode = os.path.join(*self.BASE_ZNODE, znode)
        else:
            self.log.raiseException('Unsupported znode type %s (znode %s)' % (znode, type(znode)))

        self.log.debug("znode %s" % znode)
        return znode

    def make_znode(self, znode=None):
        """Make a znode"""
        znode_path = self.znode_path(znode)
