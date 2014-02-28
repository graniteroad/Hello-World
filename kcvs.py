#!/usr/bin/python

import os
import shelve
from tempfile import NamedTemporaryFile

class KeyColumnValueStore(object):

	def __init__(self, path=None):
	   self._kcvstore = {}
	   self._key = 'data'
	   self._persist_to_disk(path)

	def _persist_to_disk(self, path):
	   if path is None:
		# built in controled execution
		with NamedTemporaryFile(delete=False) as persist:
			self.path = persist.name + '.db'
	   else:
		self.path = os.path.abspath(path)
	   if os.path.isfile(self.path) and os.stat(self.path).st_size:
            	self.load()
           else:
            	self.persist()  


	def load(self):
            """Reads the existing key/column/value structure from disk."""
            s = shelve.open(self.path)
            self._kcvstore = s[self._key]
	    s.close()
	
	def persist(self):
	    s = shelve.open(self.path)
	    s[self._key] = self._kcvstore
	    s.close()

	
	def set(self, key, col, val):
           """ sets the value at the given key/column """
 	   if key in self._kcvstore:
	   	self._kcvstore[key][col] = val
	   else:
		self._kcvstore[key] = {col:val}
	   #print 'kcvstore = ', self._kcvstore[key][col]
	   self.persist()

        def get(self, key, col = None):
            """ return the value at the specified key/column """
	    if self._kcvstore.get(key) is None:
            	return None  
	    else:
		return self._kcvstore.get(key).get(col)
	    
	    # alternate way 1 liner
	    #return None if self._kcvstore.get(key) is None else self._kcvstore.get(key).get(col)

        def get_key(self, key):
            """ returns a sorted list of column/value tuples """
	    if key in self._kcvstore:
		return sorted(self._kcvstore[key].items())
	    else:
		return None

        def get_keys(self):
            """ returns a set containing all of the keys in the store """
	    return self._kcvstore.keys()

        def delete(self, key, col):
            """ removes a column/value from the given key """
	    del self._kcvstore[key][col]
	    self.persist()

        def delete_key(self, key):
            """ removes all data associated with the given key """
	    del self._kcvstore[key]
	    self.persist()
	
	def get_slice(self, key, start = None, stop = None):
            """
            returns a sorted list of column/value tuples where the column
            values are between the start and stop values, inclusive of the
            start and stop values. Start and/or stop can be None values,
            leaving the slice open ended in that direction
            """
	    # sort columns into list
	    cols = sorted(self._kcvstore.get(key).keys())
	    start_index = 0 if start is None else cols.index(start)
	    # stop_index add 1 to include stop column in slice
   	    stop_index = len(cols) + 1 if stop is None else cols.index(stop) + 1
	    return [(c, self.get(key,c)) for c in cols[start_index:stop_index]]

"""Tests"""
# Level 1
# given this store and dataset
store = KeyColumnValueStore()
store.set('a', 'aa', 'xx')
store.set('a', 'ab', 'xy')
store.set('c', 'cc', 'xr')
store.set('c', 'cd', 'xt')
store.set('d', 'de', 'xu')
store.set('d', 'df', 'xb')

# the statements below will evaluate to True
"""
if store.get('a', 'aa') == 'xx':
	print 'True'
else:
	print 'False'


if store.get_key('a') == [('aa', 'xx'), ('ab', 'xy')]:
	print 'True'
else:
 	print 'False'

"""

# nonexistent keys/columns, the statements below
# will evaluate to True
"""
if store.get('d', 'df') is None:
	print 'True'
else:
	print 'False'

if store.get('z') == []:
	print 'True'
else:
	print 'False'
"""

# if we set different values on the 'a' key:
store.set('a', 'aa', 'y')
store.set('a', 'ab', 'z')

# the statements below will evaluate to True
if store.get('a', 'aa') == 'y':
	print 'True'
else:
	print 'False'

if store.get_key('a') == [('aa', 'y'), ('ab', 'z')]:
	print 'True'
else:
	print 'False'

# deleting
store.delete('d', 'df')
if store.get('d', 'df') is None:
	print 'True'
else:
	print 'False'

# this will evaluate to True
if store.get_key('d') == [('de', 'xu')]:
	print 'True'
else:
	print 'False'

# delete an entire key
store.delete_key('c')
if store.get_key('c') is None:
	print 'True'
else:
	print 'False'

# Level 2
# given this store and dataset
store.set('a', 'aa', 'x')
store.set('a', 'ab', 'x')
store.set('a', 'ac', 'x')
store.set('a', 'ad', 'x')
store.set('a', 'ae', 'x')
store.set('a', 'af', 'x')
store.set('a', 'ag', 'x')

if store.get_slice('a', 'ae') == [('ae', 'x'), ('af', 'x'), ('ag', 'x')]:
	print 'True'
else:
	print 'False'
if store.get_slice('a', 'ae', None) == [('ae', 'x'), ('af', 'x'), ('ag', 'x')]:
	print 'True'
else:
	print 'False'
if store.get_slice('a', None, 'ac') == [('aa', 'x'), ('ab', 'x'), ('ac', 'x')]:
	print 'True'
else:
	print 'False'




