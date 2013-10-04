#!/usr/bin/python

import kazoo.security
from vsc.utils import fancylogger
from vsc.zk.base import VscKazooClient

def main():
    """
    main method
    Makes tree of znodes with ACLs on
    """
    logger = fancylogger.getLogger()
    
   
    
    # paths to add:
    pathlist= ['/p1','/p2','/p3','/p1/c1','p1/c2','p1/c2/gc1']
    
    #define ACLs for new tree
    acll=[]
    acl_tree = make_digest_acl('tree','w00f', all=True)
    acl_ro = make_digest_acl('reader','w00f', read=True) 
    acl_debug = make_digest_acl('db','ok', all=True)
    acll.append(acl_tree)
    acll.append(acl_ro)
    acll.append(acl_debug)  
    
    # initial authentication credentials for creation on root level
    ccreds = ('digest','create:w00f') # for creating tree top under root
    
    #Create kazoo/zookeeper connection
    zk = VscKazooClient(hosts='127.0.0.1:2181', default_acl=acll, auth_data=ccreds)
    
    # create a tree root node, with authentication
    zk.make_znode(value="tree top")
    # create some child nodes
#    for path in pathlist:
#      zk.make_znode(path)
      
    # get the values of some nodes

    # cleanup
    
