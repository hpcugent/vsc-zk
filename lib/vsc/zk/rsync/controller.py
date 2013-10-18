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

from vsc.zk.base import VscKazooClient

class RsyncController(VscKazooClient):

    BASE_ZNODE = '/admin/rsync'
    BASE_PARTIES = ['allsd']
    
    def __init__(self, hosts, session=None, name=None, default_acl=None, auth_data=None):
        
        self.ready= False
        kwargs = {
            'hosts'       : hosts,
            'session'     : session,
            'name'        : name,
            'default_acl' : default_acl,
            'auth_data'   : auth_data,
        }
        super(RsyncController, self).__init__(**kwargs)
        
    def get_all_hosts(self):
        hosts = []
        for host in self.parties['allsd']:
            hosts.append(host)
        return hosts        
   
    def set_ready(self):
        self.ready = True
    
    def is_ready(self):
        return self.ready
