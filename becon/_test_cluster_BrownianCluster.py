# TODO: put in semi-random-seed
import numpy as np

from cluster_BrownianCluster import BrownClusterModel
from cluster_BrownianCluster_util import PyPlotDrawHandler_BCM
from test_util import compareFloatValues

from copy import deepcopy

global verbose
global testNum

def getNewBCM_Tiled(description=None, array1=[0,1], array2=[2,3]):
	n = 1000
	tileArray = np.reshape(np.tile(np.array(array1),n),(2*n,1))
	tileArray = np.vstack((tileArray,np.reshape(np.tile(np.array(array2),n),(2*n,1))))
	
	if description is None:
		description = ''.join(map(str,BCM.wordSequence[2*n-5:2*n+5]))
	else:
		description = description
	return __getNewBCM(tileArray, description)

def __getNewBCM(data, description=None):
	BCM = BrownClusterModel()
	BCM.addTrainingData(data)
	BCM.description = description # Defined Only Here... Kind of sloppy.
	return BCM	

def testEdgeCost(BCM,expectedTotalEdgeCost=2.0):
	global verbose
	global testNum
	BCM.resetData_defineClusterCostTable()

	totalEdgeCost = sum(BCM.clusterCostTable.values())

	if compareFloatValues(totalEdgeCost, totalEdgeCost, numDigits=2):
		print 'Test {0} OK'.format(testNum)
		if verbose:
			print '\tCode type {0} total cluster cost OK'.format(BCM.description)
			print '\t(found cost of {0}, expected cost of {1})'.format(totalEdgeCost, expectedTotalEdgeCost)
	else:
		print 'Test {0} FAILED'.format(testNum)
		if verbose:
			print '\tCode type {0} total cluster cost FAILED'.format(BCM.description)
			print '\t(found cost of {0}, expected cost of {1})'.format(totalEdgeCost, expectedTotalEdgeCost)
	testNum += 1

def testMergeReductionCost(word1=(0,), word2=(1,), expectedMergeReductionCost=1.0):
	global verbose
	global testNum
	
	BCM.resetData_definemergeCostReductions()
	cluster1 = BCM.wordClusterMapping[word1]
	cluster2 = BCM.wordClusterMapping[word2]
	mergeReductionCost = BCM.mergeCostReductions.get(cluster1, cluster2)
	if compareFloatValues(mergeReductionCost, expectedMergeReductionCost, numDigits=2):
		print 'Test {0} OK'.format(testNum)
		if verbose:
			print '\tCode type {2} merge reduction cost OK for words {0} and {1}'.format(word1, word2, BCM.description)
			print '\t(found cost of {0}, expected cost of {1})'.format(mergeReductionCost, expectedMergeReductionCost)
	else:
		print 'Test {0} FAILED'.format(testNum)
		if verbose:
			print '\tCode type {2} merge reduction cost FAILED for words {0} and {1}'.format(word1, word2, BCM.description)
			print '\t(found cost of {0}, expected cost of {1})'.format(mergeReductionCost, expectedMergeReductionCost)
	testNum += 1


def testMergeCostReductionTableNaive(BCM):
	global verbose
	global testNum

	currentGraphCost = BCM.findTotalClusteringCost()
	allOK = True
	for potentialMerge, costReduction in BCM.mergeCostReductions.iteritems():
		(c1, c2) = potentialMerge
		BCM2 = deepcopy(BCM)
		BCM2.mergeClusters_changeNGramCounts(c1, c2)
		newGraphCost = BCM2.findTotalClusteringCost()
		calculatedCostReduction = newGraphCost-currentGraphCost
		if  compareFloatValues(calculatedCostReduction, costReduction):
			if verbose:
				print 'OK on potentialMerge({0},{1})'.format(c1, c2)
				print 'Expected costReduction={0}, calculatedCostReduction={1}'.format(costReduction,calculatedCostReduction)
		else:
			if verbose:
				print 'FAILED on potentialMerge({0},{1})'.format(c1, c2)
				print 'Expected costReduction={0}, calculatedCostReduction={1}'.format(costReduction,calculatedCostReduction)
			allOK = False
	if allOK:
		print 'Test {0} OK'.format(testNum)
	testNum += 1

if __name__ == '__main__':

	global verbose
	global testNum
	verbose = False
	testNum = 1

	doVisualTests = 1
	doMergeCostReductionTests = 0
	doEdgeCostTests = 0

	if doVisualTests:
		drawer = PyPlotDrawHandler_BCM()
		drawer.drawMergeSequenceUsingPyPlot()

	if doMergeCostReductionTests:
		array1 = [0,1]
		array2 = [2,3] 
		BCM = getNewBCM_Tiled(None, array1=array1, array2=array2)		
		leftToMerge=True
		while leftToMerge:
			testMergeCostReductionTableNaive(BCM)
			leftToMerge = BCM.mergeClusters_mergeTop()
	
	if doEdgeCostTests:
		array1 = [1,1]
		array2 = [1,1] 
		BCM = getNewBCM_Tiled(None, array1=array1, array2=array2)
		expectedTotalEdgeCost = 0.0
		testEdgeCost(BCM, expectedTotalEdgeCost=expectedTotalEdgeCost)
		BCM = None #Try to delete


		array1 = [0,1]
		array2 = [0,1] 
		BCM = getNewBCM_Tiled(None, array1=array1, array2=array2)
		expectedTotalEdgeCost = 1.0
		testEdgeCost(BCM, expectedTotalEdgeCost=expectedTotalEdgeCost)
		word1 = (0,)
		word2 = (1,)
		expectedMergeReductionCost = -1.0
		testMergeReductionCost(word1, word2, expectedMergeReductionCost)
		BCM = None #Try to delete

		array1 = [1,1]
		array2 = [2,3] 
		BCM = getNewBCM_Tiled(None, array1=array1, array2=array2)
		expectedTotalEdgeCost = 1.5
		testEdgeCost(BCM, expectedTotalEdgeCost=expectedTotalEdgeCost)
		word1 = (1,)
		word2 = (2,)
		expectedMergeReductionCost = -1.37
		testMergeReductionCost(word1, word2, expectedMergeReductionCost)
		word1 = (2,)
		word2 = (3,)
		expectedMergeReductionCost = -0.5 
		testMergeReductionCost(word1, word2, expectedMergeReductionCost)	
		BCM = None #Try to delete

		array1 = [0,1]
		array2 = [2,3] 
		BCM = getNewBCM_Tiled(None, array1=array1, array2=array2)
		expectedTotalEdgeCost = 2.0
		testEdgeCost(BCM, expectedTotalEdgeCost=expectedTotalEdgeCost)
		word1 = (2,)
		word2 = (3,)
		expectedMergeReductionCost = -0.5 
		testMergeReductionCost(word1, word2, expectedMergeReductionCost)	
		word1 = (0,)
		word2 = (1,)
		expectedMergeReductionCost = -0.5 
		testMergeReductionCost(word1, word2, expectedMergeReductionCost)	
		word1 = (0,)
		word2 = (2,)
		expectedMergeReductionCost = -1.0 
		testMergeReductionCost(word1, word2, expectedMergeReductionCost)	
		BCM = None #Try to delete