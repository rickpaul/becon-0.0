import numpy as np
from logging import info
from math import ceil as ceil

class CorrelatedReturnHistoryGenerator:
	def __init__(self, numAssets=5, stringLength=480, addZeroPrefix=True):
		#assets return between 0% and 5% p.a.
		returnRange = 1/20.0
		minReturn = 0.0
		#assets have between 0% and 30% std p.a.
		stdRange = 1/4.0
		minStd = 0.05
		#assets have between 0.2 and 0.7 correlation
		corrRange = 1/2.0
		minCorr = 0.2

		self.means = (1+np.random.random((numAssets))*returnRange+minReturn)**(1/12.0)-1
		std = (np.random.random((numAssets,1))*stdRange + minStd)/(12.0**.5)
		correl = np.random.random((numAssets,numAssets))*corrRange+minCorr
		correl[range(numAssets),range(numAssets)]=1
		upper = np.triu_indices(numAssets,1)
		lower = (upper[1],upper[0])
		correl[upper] = correl[lower]
		self.covs = (std * std.T)	* correl

		# Can do Cholesky decomposition method, or... (naive)
		history = np.random.multivariate_normal(self.means, self.covs, size=stringLength)
		history = history + self.__generateRandomMultivarateNormalTile(stringLength, 2)
		history = history + self.__generateRandomMultivarateNormalTile(stringLength, 7)
		history = history + self.__generateRandomMultivarateNormalTile(stringLength, 13)
		history = history + self.__generateRandomMultivarateNormalTile(stringLength, 14) # Do one that correlates with previous, to mimic sudden changes
		history = history + self.__generateRandomMultivarateNormalTile(stringLength, 91) # Do one that correlates with previous, to mimic sudden changes
		history = history / 6.0 # because we add multiple histories together

		self.history = np.cumsum(history[0:stringLength],0)
		if addZeroPrefix:
			self.history = np.vstack((np.zeros(shape=(1,numAssets)),self.history))


	def __generateRandomMultivarateNormalTile(self, stringLength, tileSize):
		length = int(ceil(stringLength/(tileSize*1.0)))
		excess = int(length * tileSize - stringLength)
		if excess:
			return np.repeat( np.random.multivariate_normal(self.means, self.covs,size=length) 
							,tileSize,0)[:-excess] / (tileSize*1.0)
		else:
			return np.repeat( np.random.multivariate_normal(self.means, self.covs,size=length) 
							,tileSize,0) / (tileSize*1.0)		

class SemiRandomSeedGenerator:
	def __init__(self, seedOverride=None):
		np.random.seed(seed=None)
		rnd_seed = (np.random.randint(9999) if seedOverride is None else seedOverride)
		info('Generating Semi Random Numbers With Seed: ' + str(rnd_seed))
		np.random.seed(seed=rnd_seed)
		self.seed = rnd_seed

class RollingVolatilityWindow:
	def __init__(self, data, window):
		self.data = data
		self.dataLength = len(data)
		self.windowSize = window*1.0
		self.trailingVols = np.empty((self.dataLength - self.windowSize,1))
		self.currentSumY = sum(data[0:self.windowSize])
		self.currentSumSqY = sum([y**2 for y in data[0:self.windowSize]])
		self.currWindowStart = 0
		# Vol = E(X^2) - E(X)^2
		self.trailingVols[0] = self.currentSumSqY/self.windowSize - (self.currentSumY/self.windowSize)**2
		self.__findTrailingVolatilities()

	def __findTrailingVolatilities(self):
		for i in range(int(self.dataLength - self.windowSize - 1)):
			self.__moveWindowOneStep()

	def __moveWindowOneStep(self):
		oldY = self.data[self.currWindowStart]
		newY = self.data[self.currWindowStart + self.windowSize]
		self.currentSumY += (newY - oldY)
		self.currentSumSqY += (newY**2 - oldY**2)
		self.currWindowStart += 1
		# Vol = E(X^2) - E(X)^2
		self.trailingVols[self.currWindowStart] = self.currentSumSqY/self.windowSize - (self.currentSumY/self.windowSize)**2