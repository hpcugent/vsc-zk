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
vsc-zk zkinitree

@author: Kenneth Waegeman (Ghent University)
"""

from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.base import VscKazooClient

options = {
     'servers':('list of zk servers', 'strlist', 'store', None)  
}
go = simple_option(options)
logger = fancylogger.getLogger()

rootinfo = go.configfile_remainder.pop('root', {})
if 'passwd' not in rootinfo:
    logger.error('Root user not configured or has no password attribute!')
rpasswd = rootinfo['passwd']
rpath = rootinfo.get('path', '/')

znodes = {}
users = {}

# Parsing config sections
for sect in go.configfile_remainder:
    if sect.startswith('/'):
        # Parsing paths
        znode = znodes.setdefault(sect, {})
        for k,v in go.configfile_remainder[sect].iteritems():
            if v: # don't parse empty parameters
                znode[k] = v.split(',')
    else:
        # Parsing users  
        user = users.setdefault(sect, {})
        for k,v in go.configfile_remainder[sect].iteritems():
            user[k] = v.split(',')
        if 'passwd' not in user:
            logger.error('User %s has no password attribute!' % sect)
    
logger.debug("znodes: %s" % znodes)
logger.debug("users: %s" % users)

#Connect to zookeeper
# initial authentication credentials and acl for admin on root level
acreds = [('digest', 'root:'+rpasswd)] 
root_acl = make_digest_acl('root', rpasswd, all=True)
#Create kazoo/zookeeper connection with root credentials
servers = go.options.servers
zkclient = VscKazooClient(servers, auth_data=acreds)
      
#Iterate paths
for path, attrs in znodes.iteritems():
    logger.debug("path %s attribs %s" % (path, attrs))
    acls = {arg:attrs[arg] for arg in attrs if arg not in ('value','ephemeral','sequence','makepath')}
    acl_list = [root_acl]
    # Parse ACLs
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
    kwargs = {arg:attrs[arg] for arg in attrs if arg in ('ephemeral','sequence','makepath')}
    if not zk.exists_znode(path):
        zkclient.make_znode(path, value=attrs.get('value',''), acl=acl_list, **kwargs)
    else:
        logger.warning('node %s already exists' % path)
        zkclient.znode_acls(path, acl_list)
