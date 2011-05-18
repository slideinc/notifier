# -*- Mode: Python; tab-width: 4 -*-

# Copyright (c) 2005-2011 Slide, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#     * Neither the name of the author nor the names of other
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys

import access


class Resolver(object):
	def __init__(self, notifier):
		self._nodes = {}
		self._roots = set()
		self._seq_owners = {}
		self._inflight = set()
		self._lastkey = 1
		self._sa = access.SplitAccess(notifier)

		# stats
		self._maxparallel = 0
		self._total_rpcs = 0

	def put(self, func=None, parents=None, rpc=True):
		'''create a new (currently) leaf node in the dependency graph.

		func is a function that accepts a notifier and a list of lists
		containing the results of its parents, and calls one or more rpc
		methods returning the sequence id(s), or None (in which case dependent
		nodes will also be aborted).

		parents is a list of keys of nodes on which this depends.

		if rpc is False:
		 - the function won't get a notifier, just the parents' results
		 - the functions return value will be the final result value for this
		   node, not sequence ids that would get completed before children run

		this may also be used as a decorator with or without preloaded
		`parents` and/or `rpc` arguments, but in that case it has the mind
		bending behavior of turning the function variable into an integer
		node id.

		so these two examples are equivalent:

		>>> node = resolver.put(lambda _p, empty: ['bar'])

		>>> @resolver.put
		... def node(_p, empty):
		...     return ['foo']

		as are these two:

		>>> node = resolver.put(lambda _p, results: ['foo'], parents=parent)

		>>> @resolver.put(parents=parent)
		... def node(_p, results):
		...     return ['foo']

		'''
		if func is None:
			return lambda func: self.put(func, parents, rpc)

		plural_parents = True
		if parents:
			if not hasattr(parents, "__iter__"):
				plural_parents = False
				parents = [parents]
		else:
			parents = []

		key = self._lastkey
		self._lastkey += 1

		if not parents:
			self._roots.add(key)

		aborted = False
		start = True
		unfinished_parents = set()
		finished = False
		exc_info = None
		for parent in parents:
			parent = self._nodes[parent]

			parent['children'].append(key)

			if parent['aborted']:
				aborted = finished = True
				start = False

			if start and not parent['finished']:
				start = False

			if not parent['finished']:
				unfinished_parents.add(parent['key'])

			if parent['exc_info'] is not None:
				exc_info = parent['exc_info']

		node = {
			'key': key,
			'func': func,
			'parents': parents,
			'children': [],
			'seqs': [],
			'intransit': set(),
			'results': {},
			'aborted': aborted,
			'finished': finished,
			'unfinished_parents': unfinished_parents,
			'plural_parents': plural_parents,
			'plural_seqs': True,
			'is_map': not rpc,
			'exc_info': exc_info,
		}
		self._nodes[key] = node

		# if all this node's dependencies are complete and
		# have passed, go ahead and fire off the rpc request
		if start:
			self._start(key)

		return key

	def get(self, key):
		'retrieve (maybe waiting for) the result associated with *key* node'
		node = self._nodes[key]

		if node['finished']:
			if node['aborted'] and node['exc_info']:
				raise node['exc_info'][0], node['exc_info'][1], node['exc_info'][2]
			return self._results(key)

		# start the roots and let constraint propogation take care of the rest
		for node_key in self._roots:
			self._start(node_key)

		while not node['finished']:
			try:
				seq, result = self._sa.any(set(self._inflight))
			except Exception, exc:
				if isinstance(exc, access.SplitServerError):
					seq = exc.args[0][0]
					owner = self._seq_owners[seq]
					self._abort(owner, sys.exc_info())
				raise
			else:
				owner = self._landing(seq, result)

		return self._results(key)

	def resolve_all(self):
		leaves = [k for k, d in self._nodes.iteritems() if not d['children']]
		for leaf in leaves:
			self.get(leaf)

	def _start(self, key):
		node = self._nodes[key]

		if node['aborted'] or node['seqs'] \
				or node['unfinished_parents'] or node['finished']:
			return

		parents_results = [self._results(p) for p in node['parents']]
		if not node['plural_parents']:
			parents_results = parents_results[0]

		seqs = None
		try:
			if node['is_map']:
				node['results'] = node['func'](parents_results)
				self._finish(node['key'])
				return
			else:
				seqs = node['func'](self._sa, parents_results)
		except Exception, exc:
			self._abort(key, sys.exc_info())

		if seqs is None:
			self._abort(key)
			return

		if not hasattr(seqs, "__iter__"):
			node['plural_seqs'] = False
			seqs = [seqs]

		for seq in seqs:
			self._seq_owners[seq] = key

		node['seqs'] = seqs
		node['intransit'] = set(seqs)
		self._inflight.update(seqs)

		self._maxparallel = max(self._maxparallel, len(self._inflight))
		self._total_rpcs += len(seqs)

		if hasattr(seqs, "__iter__") and not seqs:
			self._finish(node['key'])

	def _abort(self, key, exc_info=None):
		node = self._nodes[key]
		node['aborted'] = node['finished'] = True
		if exc_info is not None:
			node['exc_info'] = exc_info

		# eagerly abort descendents
		for desc in self._descendents(key):
			self._abort(desc, exc_info=exc_info)

	def _landing(self, seq, result):
		node = self._nodes[self._seq_owners[seq]]
		node['results'][seq] = result
		node['intransit'].remove(seq)
		self._inflight.remove(seq)
		if not node['intransit']:
			self._finish(node['key'])
		return node['key']

	def _finish(self, key):
		node = self._nodes[key]
		node['finished'] = True
		for seq in node['seqs']:
			del self._seq_owners[seq]

		for child in node['children']:
			child = self._nodes[child]
			child['unfinished_parents'].remove(key)
			if not child['unfinished_parents']:
				self._start(child['key'])

	def _results(self, key):
		node = self._nodes[key]
		if node['aborted']:
			return None
		if node['is_map']:
			return node['results']
		if node['plural_seqs']:
			return [node['results'][s] for s in node['seqs']]
		return node['results'][node['seqs'][0]]

	def _ancestors(self, key, exclude=None):
		# get all ancestors of a node recursively and lazily
		node = self._nodes[key]
		exclude = exclude or set()
		for parent in node['parents']:
			if parent not in exclude:
				yield parent
				exclude.add(parent)
		for parent in node['parents']:
			for ancestor in self._ancestors(parent, exclude):
				yield ancestor

	def _descendents(self, key, exclude=None):
		# get all descendents of a node recursively and lazily
		node = self._nodes[key]
		exclude = exclude or set()
		for child in node['children']:
			if child not in exclude:
				yield child
				exclude.add(child)
		for child in node['children']:
			for descendent in self._descendents(child, exclude):
				yield descendent

	def _validate(self, node):
		'catches cycles and stuff'
		desc = set(self._descendents(node))
		ancest = set(self._ancestors(node))
		assert not (desc & ancest)
		assert node['key'] not in desc
		assert node['key'] not in ancest
		assert set(self._nodes.keys()).issuperset(
				set([node['key']]) | desc | ancest)

	def dot_graph(self):
		dot_lines = ["digraph {"]
		for node in self._nodes.values():
			dot_lines.append('\t%d [label="%d: %s"]' % (
				node['key'], node['key'], node['is_map'] and "map" or "rpc"))

		for node in self._nodes.values():
			for parent in node['parents']:
				dot_lines.append("\t%d -> %d" % (
					self._nodes[parent]['key'], node['key']))

		dot_lines.append("}")

		return "\n".join(dot_lines)
