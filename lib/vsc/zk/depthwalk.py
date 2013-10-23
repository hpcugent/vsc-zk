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
vsc-zk depthwalk

@author: Kenneth Waegeman (Ghent University)
"""

import os
from vsc.utils import fancylogger

logger = fancylogger.getLogger()

def depthwalk(path, depth=1):
    path = path.rstrip(os.path.sep)
    assert os.path.isdir(path)
    pathslashcount = path.count(os.path.sep)
    for root, dirs, files in os.walk(path):
        yield root, dirs, files
        slashcount = root.count(os.path.sep)
        if pathslashcount + depth <= slashcount:
            del dirs[:]

def get_pathlist(path, depth):
    pathlist = [path]
    for root, dirs, files in depthwalk(path, depth):
        for name in dirs:
            pathlist.append(os.path.join(root, name))
    logger.debug("pathlist is %s" % pathlist)
    return pathlist

if __name__ == '__main__':  # for testing purposes

    get_pathlist('/tmp/test', depth=2)
