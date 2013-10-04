#!/usr/bin/python

import kazoo.security
from vsc.utils import fancylogger
from kazoo.security import KazooClient

def

def main():
    """
    main method
    Makes tree of znodes with ACLs on
    """
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
    acreds = ('digest','root:admin') # for administrating root level
    
    #Create kazoo/zookeeper connection
    zk = VscKazooClient(hosts='127.0.0.1:2181', auth_data=acreds)
    
    # set ACL on root node, with authentication
    zk.set_acls('/', acll)

