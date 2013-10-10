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
vsc-zk parser

@author: Kenneth Waegeman (Ghent University)
"""
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger

logger = fancylogger.getLogger()

def get_rootinfo(configremainder):
    """get rootpasswd and rootpath"""
    rootinfo = configremainder.pop('root', {})
    if 'passwd' not in rootinfo:
        logger.error('Root user not configured or has no password attribute!')
    rpasswd = rootinfo['passwd']
    rpath = rootinfo.get('path', '/')
    return rpasswd,rpath
    
def parse_zkconfig(configremainder):
    """get znodes and users dict"""
    znodes = {}
    users = {}

    # Parsing config sections
    for sect in configremainder:
        if sect.startswith('/'):
            # Parsing paths
            znode = znodes.setdefault(sect, {})
            for k,v in configremainder[sect].iteritems():
                if v: # don't parse empty parameters
                    znode[k] = v.split(',')
        else:
            # Parsing users  
            user = users.setdefault(sect, {})
            for k,v in configremainder[sect].iteritems():
                user[k] = v.split(',')
            if 'passwd' not in user:
                logger.error('User %s has no password attribute!' % sect)
    
    return znodes, users    

def parse_acls(acls, users, root_acl):
    """make list of ACLs out of dict"""
    acl_list = [root_acl]
    for acl, acl_users in acls.iteritems():
        if acl == 'all': 
            acl_r, acl_w, acl_c, acl_d, acl_a = True, True, True, True, True
        else:
            acl_r= 'r' in acl
            acl_w= 'w' in acl
            acl_c= 'c' in acl
            acl_d= 'd' in acl
            acl_a= 'a' in acl
      
        for acl_user in acl_users:  
            if acl_user not in users:
                logger.error('User %s not configured!' % acl_user)
            else:
                tacl = make_digest_acl(acl_user, str(users[acl_user].get('passwd')), read=acl_r, write=acl_w, create=acl_c, delete=acl_d, admin=acl_a)
                acl_list.append(tacl)
    logger.debug("acl list %s" % acl_list)
    return acl_list
