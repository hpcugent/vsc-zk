#!/usr/bin/python

from vsc.utils.generaloption import simple_option
from kazoo.security import make_digest_acl
from vsc.utils import fancylogger
from vsc.zk.base import VscKazooClient

options={
 #   'optie1':('eerste optie', <type>, 'store', <default>, 'O'),
     'servers':('list of zk servers', 'strlist','store',None)  
}
go=simple_option(options)
logger = fancylogger.getLogger()

#print go.options
#print go.configfile_remainder

rootinfo=go.configfile_remainder.pop('root',{})
if 'passwd' not in rootinfo: logger.error('Root user not configured or has no password attribute!')
rpasswd=rootinfo['passwd']
rpath=rootinfo.get('path','/')

znodes={}
users={}

# Parsing config sections
for sect in go.configfile_remainder:
    if sect.startswith('/'):
    # Parsing paths
	znode=znodes.setdefault(sect,{})
	for k,v in go.configfile_remainder[sect].iteritems():
	    if v: # lege parameters niet parsen
		znode[k]=v.split(',')
    else:
    # Parsing users  
	user=users.setdefault(sect,{})
	for k,v in go.configfile_remainder[sect].iteritems():
	    user[k]=v.split(',')
	if 'passwd' not in user: logger.error('User %s has no password attribute!' % sect)
    
     # print "znode for %s is %s : %s" % (sect, k, znode[k])

logger.debug("znodes: %s" % znodes)
logger.debug("users: %s" % users)


#Connect to zookeeper
# initial authentication credentials and acl for admin on root level
acreds = [('digest','root:'+rpasswd)] 
root_acl=make_digest_acl('root',rpasswd,all=True)
#Create kazoo/zookeeper connection with root credentials
servers=go.options.servers
zk = VscKazooClient(servers, auth_data=acreds)
    
    
    
#Iterate paths
for path, attrs in znodes.iteritems():
    logger.debug("path %s attribs %s" % (path, attrs))
    acls= {arg:attrs[arg] for arg in attrs if arg not in ('value','ephemeral','sequence','makepath')}
    acl_list=[root_acl]
    # Parse ACLs
    for acl, acl_users in acls.iteritems():
	if acl == 'all': r,w,c,d,a = True,True,True,True,True
	else:
	    r= True if 'r' in acl else False
	    w= True if 'w' in acl else False
	    c= True if 'c' in acl else False
	    d= True if 'd' in acl else False
	    a= True if 'a' in acl else False
      
	for acl_user in acl_users:	
	    if acl_user not in users:
		logger.error('User %s not configured!' % acl_user)
	    else:
		tacl=make_digest_acl(acl_user, str(users[acl_user].get('passwd')),read=r, write=r, create=c, delete=d ,admin=a)
		acl_list.append(tacl)
    logger.debug("acl list %s" % acl_list)
    kwargs= {arg:attrs[arg] for arg in attrs if arg in ('ephemeral','sequence','makepath')}
    if not zk.exists_znode(path):
	zk.make_znode(path,value=attrs.get('value',''),acl=acl_list, **kwargs)
    else:
	logger.warning('node %s already exists' % path)
	zk.znode_acls(path, acl_list)


