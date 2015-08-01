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
			log.info('...No Tasks Remain in TaskStack.')
			return False
		else:
			(fn, name, desc) = self._TaskStack.pop()
			log.info('Performing %s: %s', name, desc)
			self.__taskStack_performSingleTask(fn, name)
			return True

	def __logTimeElapsed(self, name, start, end):
		log.info('Performed %s in %d ms', name, round((end-start)*1000))

	def clear(self):
		log.info('Clearing TaskStack...')
		while self.__taskStack_performNextTask():
			pass

	def push(self, function, functionName, functionDescription):
		self._TaskStack.append((function, functionName, functionDescription))
