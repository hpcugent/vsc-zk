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
from vsc.zk.base import VscKazooClient
from vsc.utils.generaloption import simple_option

def main():
    """
    main method
    Makes a simple tree of znodes with ACLs on
    """
    go = simple_option(None)
    logger = fancylogger.getLogger()
    
    # paths to add:
    pathlist= ['p1','p2','p3','p1/c1','p1/c2','p1/c2/gc1']
    
    #define ACLs for new tree
    acll=[]
    acl_tree = make_digest_acl('tree','w00f', all=True)
    acl_ro = make_digest_acl('reader','w00f', read=True) 
    acl_debug = make_digest_acl('db','ok', all=True)
    acll.append(acl_tree)
    acll.append(acl_ro)
    acll.append(acl_debug)  
    
    # initial authentication credentials for creation on root level and building tree 
    ccreds = [('digest','create:w00f'), ('digest', 'tree:w00f')] # for creating tree top under root
    
    
    
    #Create kazoo/zookeeper connection
    zk = VscKazooClient(hosts=['127.0.0.1:2181'], default_acl=acll, auth_data=ccreds)
    
    # create a tree root node, with authentication
    if not zk.exists_znode():
      zk.make_znode(value="tree top")
    else:
      logger.debug('node already exists')
 
    # create some child nodes
    for path in pathlist:
      if not zk.exists_znode(path):
        zk.make_znode(path)
      else:
	logger.debug('node already exists')

if __name__ == '__main__':
  main()