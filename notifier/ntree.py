#
#
#
'''ntree

Provide an N level search tree where each level can have different
properties as long as the classes that define each possible level
in a search conform to the common API.

Each level registers itself by name in LEVEL_REGISTRY and the tree
creation vector will define each level using those names.
'''

LEVEL_REGISTRY = {}

def safe_and(value, mask, default = None):
    try:
        return value & mask
    except TypeError:
        pass
    try:
        return int(value) & mask
    except (ValueError, TypeError):
        pass
    try:
        value = value.lower()
    except AttributeError:
        pass

    return hash(value) & mask

class SearchTreeError(RuntimeError):
    pass

class SearchLevelError(TypeError):
    pass

class Subscription(object):
    '''Subscription

    Provide a frozen wrapper for a dictionary which allows the dictionary
    to be hashed and as a result used as a key. (objects that are inserted
    into a SearchTree need to be hashable.)
    '''

    def __init__(self, data):
        self._data = data.copy()

    def __eq__(self, o):
        return self._data.__eq__(o)
    def __ne__(self, o):
        return self._data.__ne__(o)
    def __ge__(self, o):
        return self._data.__ge__(o)
    def __gt__(self, o):
        return self._data.__gt__(o)
    def __le__(self, o):
        return self._data.__le__(o)
    def __lt__(self, o):
        return self._data.__lt__(o)
    def __cmp__(self, o):
        return self._data.__cmp__(o)
 
    def __hash__(self):
        return hash(frozenset(
            filter(lambda x: not isinstance(x[1], dict), self._data.items())))
    def __repr__(self):
        return repr(self._data)
    def __len__(self):
        return len(self._data)
    def __str__(self):
        return str(self._data)
    def __iter__(self):
        return iter(self._data)

    def __contains__(self, o):
        return o in self._data
    def __getitem__(self, key):
        return self._data[key]

    def copy(self):
        return Subscription(self._data)
    def get(self, key, default = None):
        return self._data.get(key, default)
    def has_key(self, key):
        return self._data.has_key(key)
    def items(self):
        return self._data.items()
    def keys(self):
        return self._data.keys()
    def values(self):
        return self._data.values()
    def iteritems(self):
        return self._data.iteritems()
    def iterkeys(self):
        return self._data.iterkeys()
    def itervalues(self):
        return self._data.itervalues()

class SearchLevelEmpty(object):
    def __len__(self):
        return 0

    def insert(self, *args):
        return {}

    def lookup(self, *args):
        return []

    def remove(self, *args):
        return {}

class SearchLevelEnd(object):
    '''SearchLevelEnd

    SearchTree end cap which holds the objects that are inserted into
    the tree.
    '''

    def __init__(self, name = 'end', level = 0, descendants = []):
        self._level = level
        self._name  = name
        self._data  = {}

    def __len__(self):
        return len(self._data)

    def insert(self, vector, object, data):
        self._data[object] = (object, data)
        return {}

    def lookup(self, vector):
        return self._data.values()

    def remove(self, vector, object):
        self._data.pop(object, None)
        return {}

    def values(self):
        return self._data.values()


class SearchLevel(object):
    '''SearchLevel

    Base class for real search levels.
    '''

    def __init__(self, name, level = 0, descendants = []):
        self._level = level
        self._name  = name
        self._data  = {}

        if descendants:
            self._cname, self._cklass = descendants[0]
        else:
            self._cname, self._cklass = ('end', SearchLevelEnd)

        self._desc = descendants[1:]

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return repr(self._data)

    def insert(self, vector, object, data):
        raise SearchLevelError('subclass must override')

    def lookup(self, vector):
        raise SearchLevelError('subclass must override')

    def remove(self, vector, object):
        raise SearchLevelError('subclass must override')

    def values(self):
        raise SearchLevelError('subclass must override')


class SearchLevelBasic(SearchLevel):
    '''SearchLevelBasic

    Basic search level which is a straight key/value mapping.

    An insert/remove/lookup vector is queried for the element which
    matches the name that his level was given during init. That
    element    is then used as the key for the next level in the tree.
    '''

    def _state(self, key):
        return {self._name: {
            'count': len(self._data.get(key, {})),
            'value': key }}

    def _next(self, vector):
        return self._data.get(vector.get(self._name, None), None)

    def insert(self, vector, object, data):
        key  = vector.get(self._name, None)
        desc = self._state(key)
        next = self._data.setdefault(
            key,
            self._cklass(
                self._cname,
                descendants = self._desc,
                level = self._level + 1))

        result = next.insert(vector, object, data)
        result.update(desc)
        return result

    def lookup(self, vector):
        next = self._data.get(vector.get(self._name, None), None)
        if next is not None:
            return next.lookup(vector)
        else:
            return []

    def remove(self, vector, object):
        key  = vector.get(self._name, None)
        next = self._data.get(key, None)
        if next is None:
            return {}

        result = next.remove(vector, object)

        if not len(next):
            self._data.pop(key, None)

        result.update(self._state(key))
        return result

    def values(self):
        return reduce(
            lambda j, k: j.extend(k) or j,
            map(lambda i: i.values(), self._data.values()),
            [])


class SearchLevelMask(SearchLevel):
    '''SearchLevelMask

    Provides a search level with a mask/value pair as the key to the
    next search level in the tree.

    An insert/remove vector is queried for the element which matches
    the name that his level was given during init. The element is
    expected to be a tuple of two integers, the first is a mask and
    the second a value. If the element has only a single value then
    no mask (None) is assumed, and is treated as a 'match all' mask.

    A lookup vector vector is queried for the element which matches
    the name that his level was given during init. The element is
    expected to be an integer which is masked against each mask present
    at this level and the resulting value is used to query the next
    level in the search tree.
    '''

    def _state(self, vector):
        dset = vector.get(self._name, (0, 0))
        try:
            mask, value = dset
        except TypeError, e:
            size = len(self._data.get(None, {}))
            dset = (0, 0)
        else:
            size = len(self._data.get(mask, {}).get(value, {}))

        return {self._name: {'count': size,    'value': dset}}

    def _pair(self, vector):
        dset = vector.get(self._name, (0, 0))
        try:
            mask, value = dset
        except (ValueError, TypeError), e:
            value = dset
            mask  = None

        return mask, value

    def insert(self, vector, object, data):
        mask, value = self._pair(vector)

        desc = self._state(vector)
        next = self._data.setdefault(mask, {})
        next = next.setdefault(
            value,
            self._cklass(
                self._cname,
                descendants = self._desc,
                level = self._level + 1))

        result = next.insert(vector, object, data)
        result.update(desc)
        return result

    def lookup(self, vector):
        value  = vector.get(self._name, None)
        result = []

        for mask, data in self._data.items():
            if mask is None:
                next = data.get(value, None)
            else:
                next = data.get(safe_and(value, mask), None)

            if next is None:
                continue

            result.extend(next.lookup(vector))

        return result

    def remove(self, vector, object):
        mask, value = self._pair(vector)

        ml = self._data.get(mask, None)
        if ml is None:
            return {}
            
        vl = ml.get(value, None)
        if not vl:
            return {}

        result = vl.remove(vector, object)

        if not len(vl):
            ml.pop(value, None)

        if not len(ml):
            self._data.pop(mask, None)

        result.update(self._state(vector))
        return result

    def values(self):
        return reduce(
            lambda n,m: n.extend(m) or n,
            map(
                lambda l: l.values(),
                reduce(
                    lambda j, k: j.extend(k) or j,
                    map(lambda i: i.values(), self._data.values()),
                    [])),
            [])


LEVEL_REGISTRY.update({
    'basic': SearchLevelBasic,
    'mask':  SearchLevelMask })

class SearchTree(object):
    '''SearchTree

    A new Tree is instantiated with a name/level vector. The vector is
    an ordered list of tuples. The order of the tuples define the order
    of levels in the tree. Each tuple consists of the level name, which
    will be used to reference values in a lookup/insert/remove vector,
    and a level type, which will reference the level class in the
    LEVEL_REGISTRY. 
    '''

    def __init__(self, vector):
        self._vector = map(lambda v: (v[0], LEVEL_REGISTRY.get(v[1])), vector)
        if self._vector:
            name, klass = self._vector[0]
            self._tree = klass(name, descendants = self._vector[1:])
        else:
            self._tree = SearchLevelEmpty()

    def __repr__(self):
        return repr(self._tree)

    def insert(self, vector, object, data = {}):
        return self._tree.insert(vector, object, data)

    def lookup(self, vector):
        return self._tree.lookup(vector)

    def remove(self, vector, object):
        return self._tree.remove(vector, object)

    def values(self):
        return self._tree.values()
