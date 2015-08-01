import pprint #TEST
import colorsys

import numpy as np

from cluster_BrownianCluster import BrownClusterModel
from stats_util import CorrelatedReturnHistoryGenerator
from stats_util import CorrelatedReturnHistoryGenerator
import EMF_DataGenerator_util as EM_DGUtil

from matplotlib import pyplot as plt
from matplotlib import patches as patches
from copy import deepcopy


global verbose
global testNum

def getNewBCM_Tiled(description=None, array1=[0,1], array2=[2,3]):
	BCM = BrownClusterModel()
	n = 1000
	tileArray = np.reshape(np.tile(np.array(array1),n),(2*n,1))
	tileArray = np.vstack((tileArray,np.reshape(np.tile(np.array(array2),n),(2*n,1))))
	
	if description is None:
		description = ''.join(map(str,BCM.wordSequence[2*n-5:2*n+5]))
	else:
		description = description
	return getNewBCM(tileArray, description)

def getCorrelatedReturnObject(numAssets=3, stringLength=4000):
	TDG = CorrelatedReturnHistoryGenerator(numAssets=numAssets, stringLength=stringLength)
	return TDG.history

def getFirstOrderDifferenceWords(dataSeries, periodLength=10):
	return EM_DGUtil.findFirstOrderDifferences(dataSeries, periodLength)

def getNewBCM(data, description=None):
	BCM = BrownClusterModel()
	BCM.addTrainingData(data)
	BCM.description = description
	return BCM	

def testEdgeCost(BCM,expectedTotalEdgeCost=2.0):
	global verbose
	global testNum
	BCM.defineClusterCostTable()

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
	
	BCM.defineClusterMergeCostReductionTable()
	cluster1 = BCM.wordClusterMapping[word1]
	cluster2 = BCM.wordClusterMapping[word2]
	bigramTuple = BCM.getClusterCostBigram(cluster1, cluster2)
	mergeReductionCost = BCM.clusterMergeCostReductionTable[bigramTuple]
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
	for potentialMerge, costReduction in BCM.clusterMergeCostReductionTable.iteritems():
		(c1, c2) = potentialMerge
		BCM2 = copy.deepcopy(BCM)
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

def compareFloatValues(num1, num2, numDigits=6):
	exp = 10**numDigits
	return round(num1*exp)==round(num2*exp)

def get_N_HexColors(N=5):
	HSV_tuples = [(x*1.0/N, 0.5, 0.5) for x in xrange(N)]
	hex_out = []
	for rgb in HSV_tuples:
		rgb = map(lambda x: int(x*255),colorsys.hsv_to_rgb(*rgb))
		hexString = "".join(map(lambda x: chr(x).encode('hex'),rgb))
		hex_out.append('#'+hexString)
	return hex_out

def drawClusterSequence(BCM, subplot, startSequence, endSequence, differencePeriod, colors):
	lowY = -1.5
	height = 3.0
	width = 1.0#/BCM.sequenceLength
	for i in range(startSequence, endSequence):
		subplot.add_patch(
			patches.Rectangle(
				(i+differencePeriod, lowY), width, height,
				alpha=0.5,
				facecolor=colors[BCM.clusterSequence[i]],
				edgecolor='none'
		))

if __name__ == '__main__':
	# testMergeReductionCostModification()
	global verbose
	global testNum
	verbose = False
	testNum = 1

	doVisualTests = 1
	doMergeCostReductionTests = 0
	doSimpleTests = 0


	if doVisualTests:
		doDisplay = 0
		numAssets = 3
		stringLength = 200
		differencePeriod = 10
		length = stringLength - differencePeriod
		startH = 0 # start history
		endH = startH + length + differencePeriod
		startD = startH + differencePeriod
		endD = endH

		TDG = getCorrelatedReturnObject(numAssets=numAssets, stringLength=stringLength)
		diff = getFirstOrderDifferenceWords(TDG, periodLength=differencePeriod)[startD:endD]
		BCM = getNewBCM(diff)
		BCM.toggleVerbosity(True)
		numClusters = len(BCM.clusters)
		colors = get_N_HexColors(numClusters)		
		if doDisplay:
			moreToMerge = True
			while moreToMerge:
				# Display History
				plt.figure(1)
				plt.subplot(211)
				plt.xlim(startH, endH)
				x_History = range(startH, endH)
				x_Diff = range(startD, endD)
				for asset in range(numAssets):
					plt.plot(x_History,history[startH:endH,asset])
				# Display Clusters
				subplot = plt.subplot(212)
				plt.xlim(startH, endH)
				plt.ylim(-1.5, 1.5)
				for asset in range(numAssets):
					plt.plot(x_Diff,diff[:,asset])
				drawClusterSequence(BCM, subplot, startD-differencePeriod, endD-differencePeriod, differencePeriod, colors) #hacky.
				plt.show()
				moreToMerge = BCM.mergeTopClusters(updateClusterSequence=True)
			BCM.performClustering_convertMergeHistoryToBinaryWords()
			BCM.performClustering_findInterClusterDistances()
			BCM.performClustering_establishDistanceMeasure()				
		else:
			BCM.performBrownianClustering()
		# BCM.performClustering_writeJSONToFile()

	if doMergeCostReductionTests:
		leftToMerge=True
		while leftToMerge:
			testMergeCostReductionTableNaive(BCM)
			leftToMerge = BCM.mergeTopClusters()
	
	if doSimpleTests:
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