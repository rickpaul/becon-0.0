# TODO: put in logging
# TODO: put in semi-random-seed
import numpy as np

from cluster_BrownianCluster import BrownClusterModel
from cluster_BrownianCluster_util import DrawHandler_BCM
from cluster_BrownianCluster_util import writeDictionaryToJSON
from EMF_util import webJSONRepository

import test_util as testing

from copy import deepcopy


def getNewBCM_Tiled(description=None, array1=[0,1], array2=[2,3]):
	n = 1000
	tileArray = np.reshape(np.tile(np.array(array1),n),(2*n,1))
	tileArray = np.vstack((tileArray,np.reshape(np.tile(np.array(array2),n),(2*n,1))))
	return __getNewBCM(tileArray, description)

def __getNewBCM(data, description=None):
	BCM = BrownClusterModel()
	BCM.addTrainingData(data)
	dataMidpoint = len(data)/2
	if description is None:
		description = ''.join(map(str,BCM.wordSequence[dataMidpoint-5:dataMidpoint+5]))
	BCM.description = description # Defined Only Here... Kind of sloppy.
	return BCM	

def testEdgeCost(BCM,expectedTotalEdgeCost=2.0):
	totalEdgeCost = sum([v for i,v in BCM.clusterCostTable.iteritems()])
	desc = 'Total cluster cost: BCM {0}'.format(BCM.description)
	testing.floatComparisonTest(totalEdgeCost, expectedTotalEdgeCost, 'totalEdgeCost', 'expectedTotalEdgeCost', desc, numDigits=2)

def testMergeReductionCost(word1=(0,), word2=(1,), expectedMergeReductionCost=1.0):
	cluster1 = BCM.wordClusterMapping[word1]
	cluster2 = BCM.wordClusterMapping[word2]
	mergeReductionCost = BCM.mergeCostReductions.get(cluster1, cluster2)
	desc = 'Merge reduction cost for words {0} and {1}: BCM {2}'.format(word1, word2, BCM.description)
	testing.floatComparisonTest(mergeReductionCost, expectedMergeReductionCost, 'mergeReductionCost', 'expectedMergeReductionCost', desc, numDigits=2)

def testMergeCostReductionTableNaive(BCM):
	currentGraphCost = BCM.findTotalClusteringCost()
	allOK = True
	for potentialMerge, costReduction in BCM.mergeCostReductions.iteritems():
		(c1, c2) = potentialMerge
		BCM2 = deepcopy(BCM)
		BCM2.mergeClusters_changeNGramCounts(c1, c2)
		newGraphCost = BCM2.findTotalClusteringCost()
		calculatedCostReduction = newGraphCost - currentGraphCost

		desc = 'Potential MergeCostReduction for clusters {0} and {1}: BCM {2}'.format(c1, c2, BCM.description)
		allOK = (allOK and 
			testing.floatComparisonTest(calculatedCostReduction, costReduction, 'calculatedCostReduction', 'costReduction', desc, throwawayTest=True)
			)
	if allOK:
		testing.outputTestOK('All Merge Cost Reductions Worked as Expected')
	else:
		testing.outputTestFAILED('Not All Merge Cost Reductions Worked as Expected')


if __name__ == '__main__':
	testing.resetTestSuite()

	doVisualTests = 1
	doMergeCostReductionTests = 0
	doEdgeCostTests = 0

	if doVisualTests:
		drawer = DrawHandler_BCM()
		drawDict = drawer.drawBCMUsingPyPlot(numberOfLoops=2)
		writeDictionaryToJSON(drawDict, webJSONRepository + 'test_BCMClusterGraph.json', prettyPrint=True)

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