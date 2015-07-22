import sys
import logging as log
import numpy as np
import stats_util

def testSetup():
	EM_util.initializeLog(recordLog=True)

def testVolWindow():
	stringLength=500
	history = stats_util.CorrelatedReturnHistoryGenerator(	numAssets=1, 
															stringLength=stringLength, 
															addZeroPrefix=False).history
	windowSize = 50

	VW = stats_util.RollingVolatilityWindow(history, windowSize)
	calculatedVols = VW.trailingVols
	pythonCalculatedVols = np.empty((stringLength-windowSize,1))
	for i in range(windowSize,stringLength):
		pythonCalculatedVols[i-windowSize] = np.std(history[i-windowSize:i])**2
	assert np.allclose(calculatedVols, pythonCalculatedVols, rtol=1e-05, atol=1e-08)
	log.info('Testing Rolling Volatility Window... TEST PASSED')

def testSemiRandomSeedGenerator():
	seedOverride = 100000 #outside range of semirandomgenerator
	stats_util.SemiRandomSeedGenerator(seedOverride)
	expString = \
	'''100110101000010011011000011000011010000010110011101010000101101011000101101'''
	assert expString == reduce(lambda a,b: str(a)+str(b),np.random.randint(2, size=(75)))
	log.info('Testing Semi Random Seed Generator... TEST 1 PASSED')
	stats_util.SemiRandomSeedGenerator()
	assert expString != reduce(lambda a,b: str(a)+str(b),np.random.randint(2, size=(75)))
	log.info('Testing Semi Random Seed Generator... TEST 2 PASSED')

def main():
	testSemiRandomSeedGenerator()
	testVolWindow()

if __name__ == '__main__':
	testSetup()
	main()