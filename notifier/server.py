#
#
#
"""
server convenience functions
"""

from gogreen import coro

import socket
import random
import getpass
import logging
import gc
import time

from btserv import opc # note: executes on import. do not remove.

from configs.config import config

SRV_LOG_FRMT = '[%(name)s|%(coro)s|%(asctime)s|%(levelname)s] %(message)s'

LOGLEVELS = dict(
    CRITICAL=logging.CRITICAL, DEBUG=logging.DEBUG, ERROR=logging.ERROR,
    FATAL=logging.FATAL, INFO=logging.INFO, WARN=logging.WARN,
    WARNING=logging.WARNING,INSANE=5)

PEER_VERSION_MIN  = 0
PEER_VERSION_RPC  = 1
PEER_VERSION_SUB  = 2
PEER_VERSION_LST  = 3
PEER_VERSION_ADR  = 4
PEER_VERSION_PING = 5
PEER_VERSION = PEER_VERSION_PING

ITEM_STATS_ORIGINAL = 1
ITEM_STATS_MINMAX   = 2
ITEM_STATS_VERSION  = ITEM_STATS_MINMAX

REC_STATS_ORIGINAL = 1
REC_STATS_VERSION  = REC_STATS_ORIGINAL

DBPOOL_OFFLINE_TIMEOUT = 60

class GreenFormatter(object):
    def __init__(self, fmt = SRV_LOG_FRMT, width = 0):
        self.fmt = logging.Formatter(fmt)
        self.width = width
    def set_width(self, width):
        self.width = width
    def formatTime(self, record, datefmt=None):
        return self.fmt.formatTime(record, datefmt)
    def formatException(self, ei):
        return self.fmt.formatException(ei)
    def format(self, record):
        msg = self.fmt.format(record)
        if self.width:
            return msg[:self.width]
        else:
            return msg

def gethostname():
    '''gethostname

    Return the localhost name, adding the domain if specified.
    '''

    name = (socket.gethostname(), config.internal_domain)
    return '.'.join(filter(bool, name))


def get_publish_address():
    '''get_publish_address

    Returns a list of all available publish notifier (host, pushport)
    tuples.  Locally listening notifiers run by the current user are
    shuffled to the front followed by locally listening notifiers.
    Otherwise the ordering of the list is randomized. Finally, entries
    marked as restricted are removed. (usually for load/performance
    reasons)
    '''
    
    notifiers = config.btservs.values()
    addrs = []

    user = getattr(config, 'pg_user', getpass.getuser())
    host = gethostname()

    if not notifiers:
        return [(None, None)]

    random.shuffle(notifiers)
    #
    # first look for same or no user plus localhost
    for i in xrange(len(notifiers)-1, -1, -1):
        if notifiers[i]['host'] not in [host, 'localhost']:
            continue
        
        if user != notifiers[i].get('user', user):
            continue

        addrs.append(notifiers.pop(i))
    #
    # next look for same user or no user
    for i in range(len(notifiers)-1, -1, -1):
        if user == notifiers[i].get('user', user):
            addrs.append(notifiers.pop(i))
    #
    # remove restricted entries
    addrs = filter(lambda x: not x.get('restricted', False), addrs)
    #
    # reduce entries to address tuples
    return map(lambda x: (x['host'], x['pushport']), addrs)

def get_imgnotify_address():
    """get_imgnotify_address returns a list of all available imagesrv
    notifier (host, pushport) tuples.  Locally listening notifiers are
    shuffled to the front, but, otherwise, the ordering of the list is
    random.
    """
    notifier_list = config.imagesrvs
    addr = []

    host = gethostname()

    if not notifier_list:
        return [(None, None)]
    random.shuffle(notifier_list)
    # look for localhosts
    for i in range(len(notifier_list)-1, -1, -1):
        item = notifier_list[i]
        if item[0] in [host, 'localhost']:
            addr.append(item)
            del(notifier_list[i])

    return addr + notifier_list

def get_local_servers(config_map, user = None):
    """get_local_servers returns a dictionary keyed by server ids of
    all configuration blocks from config_map for servers that run as
    the current user on the local machine.  The dictionary is keyed 
    list of configuration blocks all server configurations in
    config_map.
    """
    local_srvs = {}
    user = user or getpass.getuser()
    name = gethostname()
    for id, server in config_map.items():
        if server.get('host', 'localhost') in [name, 'localhost'] and \
            user == server.get('user', user):
            local_srvs[id] = server

    return local_srvs

def _lock(server):
    """_lock attempt to bind and listen to server's lockport.
    Returns the listening socket object on success.
    Raises socket.error if the port is already locked.
    """
    s = coro.make_socket(socket.AF_INET, socket.SOCK_STREAM)
    s.set_reuse_addr()
    s.bind((server.get('bind_ip', ''), server['lockport']))
    s.listen(1024)

    return s

def lock_node(config_map, id=None):
    if id is None:
        for id, server in get_local_servers(config_map).items():
            try:
                s = _lock(server)
            except socket.error:
                pass
            else:
                server['lock']  = s
                server['total'] = len(config_map)
                server['id']    = id
                return server
    else:
        server = get_local_servers(config_map)[id]
        try:
            s = _lock(server)
        except socket.error:
            pass
        else:
            server['lock']  = s
            server['total'] = len(config_map)
            server['id']    = id
            return server

    return None # unused value

def look_node(config_map, server_id):
    """look_node returns True if the lock port for the given server_id
    is in use by the current user on the local machine, False otherwise.
    """
    local_map = get_local_servers(config_map)
    server = local_map.get(server_id)
    if server is not None:
        try:
            s = _lock(server)
        except socket.error:
            return True
        else:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
    return False

def look_nodes(config_map):
    """look_nodes returns a dictionary of True/False values keyed by
    server_id.  If the associated value is True, the server's lock
    port is in use.  If the value is False, the server's lock port is
    available for use.
    """
    lock_map = {}
    for id, server in get_local_servers(config_map).items():
        try:
            s = _lock(server)
        except socket.error:
            lock_map[id] = True
        else:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
            lock_map[id] = False
    return lock_map


def disable_gc():
    #
    # Turn off the generational garbage collector which is used to
    # collect cyclical object references. This garbage collector is
    # expensive when dealing with as many objects as do most of our
    # servers, and under normal operation they do not have circular
    # references which require this GC
    #
    return gc.disable()

def run_gc():
    #
    # Run the generational garbage collector which has been off the entire
    # life of the process. This is just to make sure that there was nothing
    # to collect. If there were any loops found THEY MUST BE FIXED, since
    # under normal operation they will be leaks.
    #
    gc.set_debug(gc.DEBUG_STATS|gc.DEBUG_SAVEALL)
    counter = gc.collect()
    gc.set_debug(0)
    return counter

def db_offline(dbpool, request):
    '''db_offline

    Turn off a DB pool and format an appropriate response.
    '''

    start = time.time()
    if not request.get('dryrun', False):
        status = dbpool.off(
            partition = request.get('partition', None),
            timeout   = request.get('timeout', None))
    else:
        status = False

    return {
        'time':   time.time() - start,
        'status': status,
        'where':  dbpool.find()}

def db_online(dbpool, request):
    '''db_online

    Turn on a DB pool and format an appropriate response.
    '''
    dbpool.on(request.get('partition', None))
    return {}

def db_status(dbpool, request):
    '''db_status

    Return information about the DB pool
    '''
    return {
        'status': dbpool.active(),
        'where':  dbpool.find()}

def db_reload(dbpool, request):
    '''db_reload

    Set the dbpool to patient, offline the DB, reload config, online
    the DB and reset patient.
    '''
    timeout = kwargs.get('timeout', DBPOOL_OFFLINE_TIMEOUT)
    patient = dbpool.patient(True)
    try:
        start  = time.time()
        status = dbpool.off(timeout = timeout)
        try:
            if status:
                reload(sys.modules['configs.Config'])
                return {'rc': 0, 'start': time.time()-start}
            #
            # error, return missing cursor location(s)
            #
            return {
                'rc':     1,
                'msg':    'db failed to offline',
                'status': status,
                'start':  time.time()-start,
                'where':  self._dbpool.find()}
        finally:
            dbpool.on()
    finally:
        dbpool.patient(patient)
    
#
# end...
