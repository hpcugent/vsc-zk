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

@author: Kenneth Waegeman (Ghent University)
"""

from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.base import VscKazooClient


def main():
    """
    main method
    Sets ACLs on root node
    """
    go = simple_option(None)
    logger = fancylogger.getLogger()
    
    #define ACLs for root
    acll=[]
    acl_adm = make_digest_acl('root','admin', all=True)
    acl_ro = make_digest_acl('reader','w00f', read=True) 
    acl_create = make_digest_acl('create','w00f', create=True)
    acl_debug = make_digest_acl('db','ok', all=True)
    
    acll.append(acl_adm)
    acll.append(acl_ro)
    acll.append(acl_create)    
    acll.append(acl_debug)
    
    
    # initial authentication credentials for admin on root level
    acreds = [('digest','root:admin')] 
    
    #Create kazoo/zookeeper connection
    zk = VscKazooClient(hosts=['127.0.0.1:2181'], auth_data=acreds)
   
    # set ACL on root node, with authentication
    zk.set_acls('/', acll)
    logger.debug('acl set')
    
    
    zk.stop()


if __name__ == '__main__':
  main()
