#!/usr/bin/env python
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
vsc-zk zkinitree

@author: Kenneth Waegeman (Ghent University)
"""

from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.zk.base import VscKazooClient
from vsc.zk.configparser import get_rootinfo, parse_zkconfig, parse_acls

logger = fancylogger.getLogger()

def main():
    """ Builds a zookeeper tree with ACLS on from a config file"""
    options = {
        'servers':('list of zk servers', 'strlist', 'store', None)
    }
    go = simple_option(options)

    rpasswd, _ = get_rootinfo(go.configfile_remainder)
    znodes, users = parse_zkconfig(go.configfile_remainder)

    logger.debug("znodes: %s" % znodes)
    logger.debug("users: %s" % users)

    # Connect to zookeeper
    # initial authentication credentials and acl for admin on root level
    acreds = [('digest', 'root:' + rpasswd)]
    root_acl = make_digest_acl('root', rpasswd, all=True)

    # Create kazoo/zookeeper connection with root credentials
    servers = go.options.servers
    zkclient = VscKazooClient(servers, auth_data=acreds)

    # Iterate paths
    for path, attrs in znodes.iteritems():
        logger.debug("path %s attribs %s" % (path, attrs))
        acls = dict((arg, attrs[arg]) for arg in attrs if arg not in ('value', 'ephemeral', 'sequence', 'makepath'))
        acl_list = parse_acls(acls, users, root_acl)
        kwargs = dict((arg, attrs[arg]) for arg in attrs if arg in ('ephemeral', 'sequence', 'makepath'))
        if not zkclient.exists_znode(path):
            zkclient.make_znode(path, value=attrs.get('value', ''), acl=acl_list, **kwargs)
        else:
            logger.warning('node %s already exists' % path)
            zkclient.znode_acls(path, acl_list)

    zkclient.exit()

if __name__ == '__main__':
    main()
