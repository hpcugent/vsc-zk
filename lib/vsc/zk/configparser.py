# -*- coding: latin-1 -*-
#
# Copyright 2013-2016 Ghent University
#
# This file is part of vsc-zk,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-zk
#
# vsc-zk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-zk is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-zk. If not, see <http://www.gnu.org/licenses/>.
#
"""
vsc-zk parser

@author: Kenneth Waegeman (Ghent University)
"""
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger

logger = fancylogger.getLogger()

def get_rootinfo(configremainder):
    """
    takes a hierarchical dictionary of config sections 
    (see configfile_remainder attribute of simple_option)
    and returns rootpasswd and rootpath
    """
    rootinfo = configremainder.pop('root', {})
    if 'passwd' not in rootinfo:
        logger.raiseException('Root user not configured or has no password attribute!')
    rpasswd = rootinfo['passwd']
    rpath = rootinfo.get('path', '/')
    return rpasswd, rpath

def parse_zkconfig(configremainder):
    """
    takes a hierarchical dictionary of config sections 
    (see configfile_remainder attribute of simple_option)
    and returns a znodes and users dict
    """
    znodes = {}
    users = {}

    # Parsing config sections
    for sect in configremainder:
        if sect.startswith('/'):
            # Parsing paths
            znode = znodes.setdefault(sect, {})
            for k, v in configremainder[sect].iteritems():
                if v:  # don't parse empty parameters
                    znode[k] = v.split(',')
        else:
            # Parsing users
            user = users.setdefault(sect, {})
            for k, v in configremainder[sect].iteritems():
                user[k] = v.split(',')
            if 'passwd' not in user:
                logger.raiseException('User %s has no password attribute!' % sect)

    return znodes, users

def parse_acls(acls, users, root_acl):
    """takes a dictionary of acls : users and returns list of ACLs"""
    acl_list = [root_acl]
    for acl, acl_users in acls.iteritems():
        if acl == 'all':
            acl_r, acl_w, acl_c, acl_d, acl_a = True, True, True, True, True
        else:
            acl_r = 'r' in acl
            acl_w = 'w' in acl
            acl_c = 'c' in acl
            acl_d = 'd' in acl
            acl_a = 'a' in acl

        for acl_user in acl_users:
            if acl_user not in users:
                logger.raiseException('User %s not configured!' % acl_user)
            else:
                tacl = make_digest_acl(acl_user, str(users[acl_user].get('passwd')), \
                    read=acl_r, write=acl_w, create=acl_c, delete=acl_d, admin=acl_a)
                acl_list.append(tacl)
    logger.debug("acl list %s" % acl_list)
    return acl_list
