'''pyinfo

Functions for gathering and presenting information about the running
python program.
'''

import sys

def rawstack(depth = 1):
	data = []
	
	while depth is not None:
		try:
			f = sys._getframe(depth)
		except ValueError:
			depth = None
		else:
			data.append((
				'/'.join(f.f_code.co_filename.split('/')[-2:]),
				f.f_code.co_name,
				f.f_lineno))
			depth += 1

	return data

def callstack(outfunc = sys.stdout.write, depth = 1, compact = False):
	data = rawstack(depth)
	data = map(lambda i: '%s:%s:%d' % i, data)

	if compact:
		return outfunc('|'.join(data))

	for value in data:
		outfunc(value)

	return None
#
# end...



