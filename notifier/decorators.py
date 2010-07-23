import sys
import cjson
import inspect
import simplejson

from configs.config import config
from util import applog
from util import context
from util import pyinfo
from util import typed
from util.tracebacklog import tbprint

decorator_list = ['public', 'command', 'noauth']

def public(method):
	method.public = True
	return method

def requireslogin(method):
	method.public = True
	method.requireslogin = True
	return method

def auth_required(method):
	method.public = True
	method.auth_required = True
	return method

def pre_auth(method):
	method.pre_auth = True
	return method

def admin(method):
	method.public = True
	method.admin = True
	return method

def sales(method):
	method.public = True
	method.sales = True
	return method

def ss_signed(method):
	method.public = True
	method.ss_signed = True
	return method

def exclude_processing(method):
	method.public = True
	method.exclude_processing = True
	return method
	
def signed(*signed_args):
	def decorate(method):
		method.signed_args = signed_args
		return method
	return decorate

def get_signed_args(method):
	if hasattr(method, "signed_args"):
		return method.signed_args
	return []

def command(method):
	method.command = True
	return method

def noauth(method):
	method.noauth = True
	return method

def nocursor(method):
	method.nocursor = True
	return method

def cursor(*args, **kwargs):
	def function(method):
		method.cursor   = True
		method.readonly = kwargs.get('readonly', False)
		return method
	return function

def extra_dbc_hints(*hints):
	def function(method):
		method.extra_dbc_hints = hints
		return method
	return function

def nopartition(method):
	method.nopartition = True
	return method

def envelope(method):
	method.envelope = True
	return method

def urlargs(method):
	'''indicates that the dispatch method accepts arguments as url path components, e.g. on a URL path like /se/static/source/user/flags/ctx/path, the method signature should look like method_name, source, user, flags, etc..'''
	method.urlargs = True
	return public(method)

def memoize(method):
	def wrapper(self):
		results = self.__dict__.setdefault('memoized_results', {})
		if not results.has_key(method.func_name):
			results[method.func_name] = method(self)
		return results[method.func_name]
	return wrapper

class DeprecationError(Exception):
	pass

def deprecate(text):
	'''move me when util.applog is a little more programmer friendly'''
	flag = getattr(config, 'enforce_deprecation', 0)
	if flag == 1:
		applog.log.warn('%s @ %s' % (text, pyinfo.callstack(outfunc=lambda x: x, depth=2, compact=True)))
	elif flag == 2:
		raise DeprecationError(text)

def deprecated(method, message=''):
	def d1(*a1, **kw1):
		def d2(*a2, **kw2):
			deprecate('%s is deprecated%s' % (a1[0].__name__, message and (' (%s)' % message) or ''))
			return a1[0](*a2, **kw2)
		return d2
	return d1

def make_property(method):
	''' Expects a method that returns at least a named doc string and a callable getter. 
		The order of the return values is doc, fget, [fset, [fdel]]	
	'''
	d = dict( ((i,v) for i,v in enumerate(method())) )
	d2 = {'doc':d.get(0), 'fget':d.get(1), 'fset':d.get(2), 'fdel':d.get(3)}
	return property(**d2)

def ajax(**kw):
	'''
		The @ajax decorator is intended to standardize some of the return values from AJAX calls decorated with @public
		or @facebook.decorators.unsigned/signed

		A decorated method is expected to:
			return True/False  -> 'PASS'/'FAIL'
			return str		   -> 'PASS: str'
			return int		   -> 'PASS: int'
			return dict/list   -> 'PASS: cjson.encode(dict/list)'
			return (bool, obj) -> if t[0]: return 'PASS: obj' else: 'FAIL: obj'
			return (int, obj)  -> cjson.encode({'rc' : int, 'data' : obj})
	'''
	def _wrapped(method):
		def newfunc(obj, *args, **kwargs):
			def _encode(o):
				#cjson on the dev/stages don't support proper encoding
				if hasattr(cjson, '__version__') and cjson.__version__ == '1.0.5':
					return simplejson.dumps(o, encoding='utf-8')
				return cjson.encode(o, encoding='utf-8')
			def __serialize(o):
				if type(o) in [int, str, unicode]:
					return o
				return _encode(o)
			servlet = context.servlet()
			trapped = getattr(servlet, '_trapped_exceptions', lambda *a: tuple())()
			rc = None
			rcreturn = False
			try:
				# Shamelessly-based on util.typed.coerce
				spec = inspect.getargspec(method)[0][1:]
				arg = {}
				try:
					args = args and map(lambda t: arg.update({'arg' : t[1]}) or kw.get(t[1], lambda x: x)(args[t[0]]), enumerate(spec[:len(args)])) or ()
					kwargs = dict(map(lambda k: arg.update({'arg' : k}) or (k, kw.get(k, lambda x: x)(kwargs[k])), kwargs))
				except Exception, ex:
					a = ['Method: %s' % method.func_name, str(arg)]
					a.extend(list(ex.args))
					raise ex.__class__(*a)
				rcreturn = kw.pop('rcreturns', False)
				rc = method(obj, *args, **kwargs)
				if isinstance(rc, bool):
					return rc and 'PASS' or 'FAIL'
				if isinstance(rc, tuple):
					if isinstance(rc[0], bool):
						status = rc[0] and 'PASS' or 'FAIL'
						return u'%s: %s' % (status, __serialize(rc[1]))
					if isinstance(rc[0], int):
						return u'%s' % _encode({'rc' : rc[0], 'data' : rc[1]})
				if type(rc) in [int, str, unicode]:
					return u'PASS: %s' % rc
				return u'PASS: %s' % __serialize(rc)
			except trapped:
				pass
			except Exception, ex:
				if servlet and hasattr(servlet, 'traceback'):
					servlet.traceback()
				if config.stage == 'live':
					if rcreturn:
						return unicode(_encode({'rc' : -1, 'data' : 'An error has occurred, this failure has been logged'}))
					return u'FAIL: An error has occurred, this failure has been logged'
				if rcreturn:
					return unicode(_encode({'rc' : -1, 'data' : str(ex)}))
				return u'FAIL: %s' % ex
		newfunc.func_name = method.func_name	
		if hasattr(method, '_inspected_method'):
			setattr(newfunc, '_inspected_method', method._inspected_method)
		else:
			setattr(newfunc, '_inspected_method', method)
		return public(newfunc)
	return _wrapped

def function_name(method):
	def wrapper(self, *args, **kwargs):
		try:
			f = sys._getframe(1)

			classname = ''
			if 'self' in f.f_locals:
				classname = f.f_locals['self'].__class__.__name__

			setattr(context.servlet(), '__function', "%s.%s" % (classname, method.func_name))
		except:
			tbprint(
				showargs=True,
				prefix='PETS_FUNCTION_NAME_DECORATOR_FAIL',
				nostack=True,
			)

		return method(self, *args, **kwargs)
	return wrapper

