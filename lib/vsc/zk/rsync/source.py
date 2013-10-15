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
zk.rsync source

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
"""

from vsc.zk.base import VscKazooClient
from kazoo.recipe.party import Party
from kazoo.exceptions import NodeExistsError, NoNodeError, NoAuthError
from vsc.utils import fancylogger

class RsyncSource(VscKazooClient):

    BASE_ZNODE = '/admin/rsync'
    BASE_PARTIES = ['sources', 'allsd']
    
    def __init__(self, hosts, session=None, name=None, default_acl=None, auth_data=None):
        #self.log = fancylogger.getLogger(self.__class__.__name__, fname=False)
        
        kwargs = {
        'hosts'       : hosts,
        'session'     : session,
        'name'        : name,
        'default_acl' : default_acl,
        'auth_data'   : auth_data,

        }
        super(RsyncSource, self).__init__(**kwargs)
        
    def get_sources(self):
        hosts = []
        for host in self.parties['sources']:
            hosts.append(host)
        return hosts
    
    def get_all_hosts(self):
        hosts = []
        for host in self.parties['allsd']:
            hosts.append(host)
        return hosts        
   
