import logging as log
from timeit import default_timer as timer

class TaskStack:
	def __init__(self):
		self._TaskStack = []

	def __taskStack_performSingleTask(self, f, name):
		# TODO: Print out what it's doing.
		start = timer()
		returnValue = f() #in case f() returns something
		end = timer()
		self.__logTimeElapsed(name, start, end)

	# Returns: 	True if there are tasks left to perform. 
	#			False if there are no tasks left to perform.
	def __taskStack_performNextTask(self):
		if len(self._TaskStack) == 0:
			log.debug('...No Tasks Remain in TaskStack.')
			return False
		else:
			(fn, name, desc) = self._TaskStack.pop()
			log.debug('Performing %s: %s', name, desc)
			self.__taskStack_performSingleTask(fn, name)
			return True

	def __logTimeElapsed(self, name, start, end):
		log.debug('Performed %s in %d ms', name, round((end-start)*1000))

	def clear(self):
		log.debug('Clearing TaskStack...')
		while self.__taskStack_performNextTask():
			pass

	def push(self, function, functionName, functionDescription):
		self._TaskStack.append((function, functionName, functionDescription))


class SymmetricTable_FixedSize:
	def __init__(self, tableLength, includeDiagonal=False):
		self.tableLength = tableLength
		self.includeDiagonal = includeDiagonal
		if includeDiagonal:
			tableLength = (tableLength * (tableLength+1))/2
		else:
			tableLength = (tableLength * (tableLength-1))/2
		self.__table = [None]*tableLength

	def get(self, coord1, coord2, *args):
		value = self.__table[self.__findTableEntry(coord1, coord2)]
		if value is None:
			if len(args):
				return args[0]
			else:
				raise KeyError('Entry not found ({0}, {1})'.format(coord1, coord2))
		else:
			return value

	def set(self, coord1, coord2, value):
		self.__table[self.__findTableEntry(coord1, coord2)] = value

	def delete(self, coord1, coord2, *args, **kwargs):
		entry = self.__findTableEntry(coord1, coord2)
		if self.__table[entry] is not None:
			value = self.__table[entry]
			self.__table[entry] = None
			return value
		elif len(args):
			return args[0]
		else:
			if kwargs.get('silent', 0):
				return
			else:
				raise KeyError('Entry not found ({0}, {1})'.format(coord1, coord2))

	def __findTableEntry(self, coord1, coord2):
		# This is upper right triangle coordinates
		# That means Row, then Column. So if coord1 is 0, it is The First Row. 
		assert (coord1 <= self.tableLength), "Entry not possible; out of bounds."
		assert (coord2 <= self.tableLength), "Entry not possible; out of bounds."
		assert (self.includeDiagonal or (coord1 != coord2)), "Entry not possible; lies on diagonal."
		# Swap Coord1 and Coord2 if necessary
		if coord1 > coord2:
			temp = coord2
			coord2 = coord1
			coord1 = temp
		# Calculate table distance as if table were whole.
		tableEntry = coord1*self.tableLength + coord2
		# Subtract lower left triangle
		if self.includeDiagonal:
			tableEntry -= ((coord1)*(coord1+1)/2)
		else:
			tableEntry -= ((coord1+1)*(coord1+2)/2)
		return tableEntry

	def __len__(self):
		return len(filter(lambda x: x is not None, self.__table))

	def printTable(self, displayCount=None):
		blankValue = 'x'
		if displayCount is None: displayCount = self.tableLength
		assert displayCount <= self.tableLength, 'Attempting to print more than table contains.'

		for row in range(displayCount):
			if self.includeDiagonal:
				startValue = row
			else:
				startValue = row+1
			blankEntries = [blankValue]*(startValue)
			rowEntries = []
			for col in range(startValue,displayCount):
				rowEntries.append(self.__table[self.__findTableEntry(row, col)])
			print '\t'.join(map(str,blankEntries)),
			if len(blankEntries):
				print '\t',
			print '\t'.join(map(str,rowEntries))

# class BinaryTree:
# 	raise NotImplementedError()

# This is a sparse table
class SymmetricTable_Sparse:
	def __init__(self, orderMatters=True):
		self.__table = {}
		self._orderMatters = orderMatters

	def __findTableEntry(self, coord1, coord2):
		if self._orderMatters:
			return (coord1, coord2)
		else:
			return (coord1, coord2) if (coord1, coord2) in self.__table else (coord2, coord1)

	def get(self, coord1, coord2, *args):
		if len(args):
			return self.__table.get(self.__findTableEntry(coord1, coord2), args[0])
		else:
			return self.__table[self.__findTableEntry(coord1, coord2)]

	def set(self, coord1, coord2, value):
		self.__table[self.__findTableEntry(coord1, coord2)] = value

	def delete(self, coord1, coord2, *args, **kwargs):
		entry = self.__findTableEntry(coord1, coord2)
		if entry in self.__table:
			value = self.__table[entry]
			del self.__table[entry]
			return value
		elif len(args):
			return args[0]
		else:
			if kwargs.get('silent', 0):
				return
			else:
				raise KeyError('Entry not found ({0}, {1})'.format(coord1, coord2))

	def __len__(self):
		return len(self.__table)

	def items(self):
		return self.__table.items()

	def iteritems(self):
		return self.__table.iteritems()
