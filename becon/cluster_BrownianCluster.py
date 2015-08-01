#TODO: Figure out originalSequence copy snafu

import numpy as np
import logging as log

from timeit import default_timer as timer
from math import log as lgrthm
from copy import copy # Deprecated.
from collections import Counter
from operator import itemgetter

from cluster_util import TaskStack

import json
# import scipy.optimize.minimize as minimize
# import scipy.optimize.OptimizeResult as minResult

# See http://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.optimize.minimize.html#scipy.optimize.minimize
# See http://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.optimize.OptimizeResult.html#scipy.optimize.OptimizeResult

class DataCompatabilityError(Exception):
	# TODO: Why isn't this displaying correctly?
	def __init__(self, variableName, variableLength, expectedVariableLength):
		self.variableName = variableName
		self.variableLength = variableLength
		self.expectedVariableLength = expectedVariableLength
	def __str__(self):
		print variableName + ' expected dimension ' + str(expectedVariableLength) + ', found ' + str(variableLength)

class BrownClusterModel:

	START_CLUSTER_SYMBOL = -1
	NO_CLUSTER_SYMBOL = -2

	def __init__(self):
		self.wordSequence = []
		self.sequenceLength = 0
		self.TaskStack = TaskStack()

    #################### Data Addition Methods
    #TODO: Make more efficient. (You don't really need to redo the statistics every time you add data)
	def addTrainingData(self, wordSequence):
		(wordDataLength, wordDataWidth) = wordSequence.shape #Assume it comes in as numpy array
		# Data Validation / Word Data Width
		if not hasattr(self, 'wordDataWidth'):
			self.wordDataWidth = wordDataWidth
		if wordDataWidth != self.wordDataWidth:
			raise DataCompatabilityError('wordDataWidth', wordDataWidth, self.wordDataWidth)
		log.debug('Adding new word sequence of length %d...', wordDataLength)
		# Add Data. Simple Concatenation
		self.wordSequence = self.wordSequence + self.__addTrainingData_tuplefyWordSequence(wordSequence)
		# Update Sequence Length
		self.sequenceLength += wordDataLength
		# Reset Variables and Flags
		self.resetDataStatistics()
		log.debug('Added new word sequence. Total word sequence is now %d.', self.sequenceLength)

	def __addTrainingData_tuplefyWordSequence(self, wordSequence):
		wordDataLength = len(wordSequence)
		newWordSequence = [None] * wordDataLength #Pre-allocate. Probably not worthwhile...
		for i in range(wordDataLength):
			newWordSequence[i] = tuple(wordSequence[i,:])
		return newWordSequence

	def resetDataStatistics(self):
		# (Superfluous) Delete Existing Data
		self.sortedWords = []
		self.wordClusterMapping = {}
		self.clusterSequence = []
		self.clusters = []
		self.clusterNGramCounts = {}
		self.clusterCostTable = {}
		self.mergeCostReductions = {} 
		self.mergeHistory = []
		self.binaryRepresentation = {}
		self.interClusterDistance = {}

		# Add New Tasks
		self.TaskStack.push(lambda: self.resetData_definemergeCostReductions(), 'definemergeCostReductions', 'Finding Merge Cost Reduction Shortcuts')
		self.TaskStack.push(lambda: self.resetData_defineClusterCostTable(), 'defineClusterCostTable', 'Creating Cluster Cost Table (Finding Bigram Graph Edge Costs)')
		self.TaskStack.push(lambda: self.resetData_defineClusterCounts(), 'defineClusterCounts', 'Counting Cluster NGrams from Cluster Sequence')
		self.TaskStack.push(lambda: self.resetData_defineClusterSequence(), 'defineClusterSequence', 'Creating Cluster Sequence from Word-to-Cluster Mapping and Word Sequence')
		self.TaskStack.push(lambda: self.resetData_defineWordClusterMap(), 'defineWordClusterMap', 'Defining Word-to-Cluster Mapping')

		# Clear The Tasks
		self.TaskStack.clear()

	def resetData_defineWordClusterMap(self, numInitialClusters=None):
		self.sortedWords = [x[0] for x in Counter(self.wordSequence).most_common()]
		if numInitialClusters is None:
			self.wordClusterMapping = dict(zip(self.sortedWords,range(0,len(self.sortedWords))))
		else:
			raise NotImplementedError('Have not implemented non-used clusters yet.')
			self.wordClusterMapping = dict(zip(self.sortedWords[0:numInitialClusters],range(0,numInitialClusters)))
		log.debug('Defined Word Cluster Map for %d words.', self.sequenceLength)
		log.debug('%d words found.', len(self.sortedWords))
		log.debug('%d clusters created.', len(self.wordClusterMapping))

	def clusterCount_getBigramCount(self, cluster1, cluster2):
		return self.clusterNGramCounts[1].get((cluster1, cluster2), 0.0)

	def clusterCount_getClusterCount(self, cluster1):
		return self.clusterNGramCounts[0][cluster1] # Throw an error if not found

	def resetData_defineClusterCounts(self):
		self.clusterNGramCounts = self.resetData_defineClusterCounts_generic(self.clusterSequence)
		self.clusters = self.clusterNGramCounts[0].keys()
		self.originalClusters = copy(self.clusters)
		end = timer()
		log.debug('Found cluster sequence for %d words.', self.sequenceLength)
		log.debug('%d clusters were found.', len(self.clusterNGramCounts[0]))
		log.debug('%d bigrams were found.', len(self.clusterNGramCounts[1]))
		if self.NO_CLUSTER_SYMBOL in self.clusterNGramCounts[0]:
			log.debug('Unclustered words remain in the dataset.')
		else:
			log.debug('No unclustered words remain in the dataset.')

	def resetData_defineClusterCounts_generic(self, clusterSequence):
		# clusterSequence = [self.START_CLUSTER_SYMBOL] + clusterSequence
		sequenceLength = len(clusterSequence)
		clusterNGramCounts = {}
		clusterNGramCounts[0] = {}
		clusterNGramCounts[1] = {}
		clusterNGramCounts[0][clusterSequence[0]] = 1.0
		for i in range(1,sequenceLength):
			cluster = clusterSequence[i]
			bigram = (clusterSequence[i-1], cluster)
			clusterNGramCounts[0][cluster] = clusterNGramCounts[0].get(cluster, 0.0) + 1.0
			clusterNGramCounts[1][bigram] = clusterNGramCounts[1].get(bigram, 0.0) + 1.0
			#CONSIDER: Does the concept of discounted probabilities mean anything here?
		return clusterNGramCounts

	def resetData_defineClusterSequence(self):
		self.clusterSequence = self.resetData_defineClusterSequence_generic(self.wordSequence, )

	def resetData_defineClusterSequence_generic(self, wordSequence):
		sequenceLength = len(wordSequence)
		clusterSequence = [None] * sequenceLength
		for i in range(sequenceLength):
			clusterSequence[i] = self.wordClusterMapping.get(wordSequence[i], self.NO_CLUSTER_SYMBOL)
		return clusterSequence

	def resetData_defineClusterCostTable(self):
		numClusters = len(self.clusters)
		for i in range(numClusters):
			c1 = self.clusters[i]
			cnt_c1 = self.clusterCount_getClusterCount(c1)
			for j in range(i, numClusters):
				c2 = self.clusters[j]
				cost = self.clusterCost_findSingleClusterPairCost(c1, c2, cnt_c1=cnt_c1)
				self.clusterCostTable[(c1,c2)] = cost

	def clusterCost_findSingleClusterPairCost(self, c1, c2, cnt_c1=None, cnt_c2=None):
		if cnt_c1 is None : cnt_c1 = self.clusterCount_getClusterCount(c1)
		if c1 == c2:
			bigramCount = self.clusterCount_getBigramCount(c1, c1)
			return self.mutualInfo_Equation(bigramCount, cnt_c1, cnt_c1)
		else:
			if cnt_c2 is None : cnt_c2 = self.clusterCount_getClusterCount(c2)		
			bigramCount = self.clusterCount_getBigramCount(c1, c2)
			cost = self.mutualInfo_Equation(bigramCount, cnt_c1, cnt_c2)
			bigramCount = self.clusterCount_getBigramCount(c2, c1)
			cost += self.mutualInfo_Equation(bigramCount, cnt_c1, cnt_c2)
			return cost		

	def clusterCost_getClusterCostBigram(self, c1, c2):
		# Assumes one of the two is in the table...
		return (c1, c2) if (c1, c2) in self.clusterCostTable else (c2, c1)

	def clusterCost_getClusterCost(self, c1, c2):
		return self.clusterCostTable[self.clusterCost_getClusterCostBigram(c1, c2)]

	def mutualInfo_Equation(self, bigramCount, unigram1Count, unigram2Count):
		if bigramCount > 0:
			# I've been uncomfortable with logs being above/below 0, but what I'm coming to realize is that the zero
			# intercept of mutual information is where information goes from useful to not..?
			# Sample probability? or Population probability? Population.
			# n = sequenceLength - 1 # Sample probability
			n = self.sequenceLength # Population probability
			return bigramCount/n * lgrthm(n*bigramCount/unigram1Count/unigram2Count, 2)
		else:
			if unigram1Count == 0 or unigram2Count == 0:
				raise Exception('Erroneous clusters')
			return 0.0

	# This is the mutual information from treating two separate clusters as one.
	# If you want to treat 1 and 2 as A, vis-a-vis 3, then:
	# you need to account for 1->3, 2->3, 3->1, and 3->2 connections, and treat them all as A->3 and 3->A;
	# you also need to account for P(A) = P(1)+P(2)
	def mutualInfo_PairVersusAnother(self, mc1, mc2, c3, cnt_mc1=None, cnt_mc2=None, cnt_c3=None):
		if cnt_mc1 is None : self.clusterCount_getClusterCount(mc1)
		if cnt_mc2 is None : cnt_mc2 = self.clusterCount_getClusterCount(mc2)
		if cnt_c3 is None : cnt_c3 = self.clusterCount_getClusterCount(c3)
		mutualInformation = 0.0
		bigram1Count = self.clusterCount_getBigramCount(c3, mc1) # 3->1
		bigram2Count = self.clusterCount_getBigramCount(c3, mc2) # 3->2
		mutualInformation += self.mutualInfo_Equation((bigram1Count + bigram2Count), cnt_c3, (cnt_mc1+cnt_mc2)) #3->A
		bigram1Count = self.clusterCount_getBigramCount(mc1, c3) # 1->3
		bigram2Count = self.clusterCount_getBigramCount(mc2, c3) # 2->3
		mutualInformation += self.mutualInfo_Equation((bigram1Count + bigram2Count), cnt_c3, (cnt_mc1+cnt_mc2)) #A->3
		return mutualInformation

	# This is the mutual information from treating two separate clusters as one.
	# If you want to treat 1 and 2 as A, then:
	# you need to account for 1->2, 2->1, 1->1, and 2->2 connections, and treat them all as A->A;
	# you need to account for P(A) = P(1)+P(2)
	def mutualInfo_PairIntoOne(self, c1, c2, cnt_c1=None, cnt_c2=None):
		if cnt_c1 is None : cnt_c1 = self.clusterCount_getClusterCount(c1)
		if cnt_c2 is None : cnt_c2 = self.clusterCount_getClusterCount(c2)
		bigram1Count = self.clusterCount_getBigramCount(c1, c2) # 1->2
		bigram2Count = self.clusterCount_getBigramCount(c2, c1) # 2->1
		bigram3Count = self.clusterCount_getBigramCount(c1, c1) # 1<->1
		bigram4Count = self.clusterCount_getBigramCount(c2, c2) # 2<->2
		totalBigramCount = bigram1Count + bigram2Count + bigram3Count + bigram4Count
		return self.mutualInfo_Equation(totalBigramCount, (cnt_c1+cnt_c2), (cnt_c1+cnt_c2))

	# This is the mutual information from treating 4 separate clusters as two.
	# If you want to treat 1 and 2 as A, and 3 and 4 as B, then:
	# you need to account for 1->3, 1->4, 2->3, and 2->4 connections, and treat them all as A->B;
	# you need to account for 3->1, 4->1, 3->2, and 4->2 connections, and treat them all as B->A;
	# you need to account for P(A) = P(1)+P(2)
	# you need to account for P(B) = P(3)+P(4)
	def mutualInfo_PairVersusPair(self, c1, c2, c3, c4, cnt_c1=None, cnt_c2=None, cnt_c3=None, cnt_c4=None):
		if cnt_c1 is None : cnt_c1 = self.clusterCount_getClusterCount(c1)
		if cnt_c2 is None : cnt_c2 = self.clusterCount_getClusterCount(c2)
		if cnt_c3 is None : cnt_c3 = self.clusterCount_getClusterCount(c3)
		if cnt_c4 is None : cnt_c4 = self.clusterCount_getClusterCount(c4)
		mutualInformation = 0.0
		 # Group 1 to Group 2
		bigram1Count = self.clusterCount_getBigramCount(c1, c3)
		bigram2Count = self.clusterCount_getBigramCount(c1, c4)
		bigram3Count = self.clusterCount_getBigramCount(c2, c3)
		bigram4Count = self.clusterCount_getBigramCount(c2, c4)
		totalBigramCount = bigram1Count + bigram2Count + bigram3Count + bigram4Count
		mutualInformation += self.mutualInfo_Equation(totalBigramCount, (cnt_c1+cnt_c2), (cnt_c3+cnt_c4))
		# Group 2 to Group 1
		bigram1Count = self.clusterCount_getBigramCount(c3, c1)
		bigram2Count = self.clusterCount_getBigramCount(c3, c2)
		bigram3Count = self.clusterCount_getBigramCount(c4, c1)
		bigram4Count = self.clusterCount_getBigramCount(c4, c2)
		totalBigramCount = bigram1Count + bigram2Count + bigram3Count + bigram4Count
		mutualInformation += self.mutualInfo_Equation(totalBigramCount, (cnt_c1+cnt_c2), (cnt_c3+cnt_c4))
		return mutualInformation

	# This function gives the merge reduction cost for a single cluster pair using the naive algorithm.
	# This algorithm has been verified. 
	def mergeCost_SinglePair(self, c1, c2):
		cnt_c1 = self.clusterCount_getClusterCount(c1)
		cnt_c2 = self.clusterCount_getClusterCount(c2)
		clusterCostReduction = 0.0
		clusterCostAddition = 0.0
		for c3 in self.clusters:
			if c3 == c1 or c3 == c2:
				continue #deal with these separately. 
			clusterCostReduction += self.clusterCost_getClusterCost(c1, c3) # 1<->3 (Encompasses 1->3 and 3->1)
			clusterCostReduction += self.clusterCost_getClusterCost(c2, c3) # 2<->3 (Encompasses 2->3 and 3->2)
			# This is the procedure you get if you try to combine two nodes into one:
			# P(c,c')*log(P(c,c')/P(c)/P(c'))
			cnt_c3 = self.clusterCount_getClusterCount(c3)
			clusterCostAddition += self.mutualInfo_PairVersusAnother(c1, c2, c3, cnt_c1, cnt_c2, cnt_c3)
		# Deal with connections among the pair
		clusterCostReduction += self.clusterCost_getClusterCost(c1, c2) # 1<->2 (Encompasses 1->2 and 2->1)
		clusterCostReduction += self.clusterCost_getClusterCost(c1, c1) # 1<->1 
		clusterCostReduction += self.clusterCost_getClusterCost(c2, c2) # 2<->2
		clusterCostAddition += self.mutualInfo_PairIntoOne(c1, c2, cnt_c1, cnt_c2)
		return (clusterCostAddition - clusterCostReduction)

	def resetData_definemergeCostReductions(self):
		clusters = self.clusters
		numClusters = len(self.clusters)
		for i in range(numClusters):
			c1 = self.clusters[i]
			for j in range(i):
				c2 = self.clusters[j]
				mergeCost = self.mergeCost_SinglePair(c1, c2)
				self.mergeCostReductions[self.clusterCost_getClusterCostBigram(c1, c2)] = mergeCost
		end = timer()

	def mergeClusters_findMergeClusters(self):
		if len(self.mergeCostReductions) == 0: # Necessary?
			return (False, None) # Necessary?
		return (True, sorted(self.mergeCostReductions.items(), key=itemgetter(1), reverse=True)[0][0])

	# Change NGram Counts 
	# Remove BigramCount(1,2) and return value
	def mergeClusters_removeBigramCount(self, c1, c2):
		if (c1, c2) in self.clusterNGramCounts[1]:
			count = self.clusterNGramCounts[1][(c1, c2)]
			del self.clusterNGramCounts[1][(c1, c2)]
			return count
		else:
			return 0

	# Change NGram Counts 
	# Add count to BigramCount(1,2) (create if it doesn't exist)
	def mergeClusters_contributeBigramCount(self, c1, c2, addCount):
		if (c1, c2) in self.clusterNGramCounts[1]:
			self.clusterNGramCounts[1][(c1, c2)] += addCount
		else:
			self.clusterNGramCounts[1][(c1, c2)] = addCount

	# Change NGram Counts from merging 1 into 2 
	# We're deleting 2
	def mergeClusters_changeNGramCounts(self, mc1, mc2):
		# Change Unigram Counts
		self.clusterNGramCounts[0][mc1] += self.clusterCount_getClusterCount(mc2)
		del self.clusterNGramCounts[0][mc2]
		# Change Unigram Counts / Reset Saved Cluster Keys
		self.clusters = self.clusterNGramCounts[0].keys()
		# Change Bigram Counts / Change Non-Merging Clusters
		for c3 in self.clusters:
			if c3 == mc1 or c3 == mc2:
				continue #deal with these separately. 
			else:
				self.mergeClusters_contributeBigramCount(c3, mc1, self.mergeClusters_removeBigramCount(c3, mc2)) # 3->2 => 3->1
				self.mergeClusters_contributeBigramCount(mc1, c3, self.mergeClusters_removeBigramCount(mc2, c3)) # 2->3 => 1->3
		# Change Bigram Counts / Change Merging Clusters
		# Don't need to do 1->1
		bigramCount = 0
		bigramCount += self.mergeClusters_removeBigramCount(mc2, mc1) # 2->1 => 1->1
		bigramCount += self.mergeClusters_removeBigramCount(mc1, mc2) # 1->2 => 1->1
		bigramCount += self.mergeClusters_removeBigramCount(mc2, mc2) # 2->2 => 1->1
		self.mergeClusters_contributeBigramCount(mc1, mc1, bigramCount) # all => 1->1

	# Change Cluster Costs from merging 1 into 2
	# We're deleting 2
	# Depends on NEW Cluster Counts (i.e. NGram Counts)
	def mergeClusters_changeClusterCosts(self, mc1, mc2):
		new_cnt_mc1 = self.clusterCount_getClusterCount(mc1)  # Depends on NEW Cluster Counts
		# Change ClusterCost Table / Change Non-Merging Clusters
		for c3 in self.clusters:
			if c3 == mc1 or c3 == mc2:
				continue #deal with these separately. 
			else:
				cost = self.clusterCost_findSingleClusterPairCost(mc1, c3, cnt_c1=new_cnt_mc1) # Depends on NEW Cluster Counts
				self.clusterCostTable[self.clusterCost_getClusterCostBigram(mc1, c3)] = cost
				del self.clusterCostTable[self.clusterCost_getClusterCostBigram(mc2, c3)]
		# Change ClusterCost Table / Change Merging Clusters
		cost = self.clusterCost_findSingleClusterPairCost(mc1, mc1, cnt_c1=new_cnt_mc1)
		self.clusterCostTable[self.clusterCost_getClusterCostBigram(mc1, mc1)] = cost
		del self.clusterCostTable[self.clusterCost_getClusterCostBigram(mc2, mc2)]				

	def mergeClusters_mergeTop(self, updateClusterSequence=False, verbose=False):
		# 1) Find Merge Clusters
		(success, mergeClusters) = self.mergeClusters_findMergeClusters()
		if not success:
			return False
		(c1, c2) = mergeClusters
		if verbose: #TODO: Remove
			cost = self.mergeCostReductions[self.clusterCost_getClusterCostBigram(c1, c2)]
			print 'Merging Cluster {1} into Cluster {0}, with clusterCostReduction={2}'.format(c1, c2, cost)
		# 2) Change Merge Cost Reduction using OLD Ngram Counts 
		self.mergeClusters_changeMergeCostReductions_UnmergedClusters(c1, c2)
		self.mergeClusters_changeMergeCostReductions_RemoveDeletedCluster(c2)
		# 3) Change Unigram and Bigram Counts
		self.mergeClusters_changeNGramCounts(c1, c2)
		# 4) Change Cluster Cost Table
		self.mergeClusters_changeClusterCosts(c1, c2)
		# 5) Change Merge Cost Reduction using NEW Ngram Counts 
		self.mergeClusters_changeMergeCostReductions_MergedClusters(c1)
		# 6) Record Change in MergeHistory
		self.mergeHistory.append((c1,c2))
		# TODO: Skip mergeHistory; go straight to dictionaryrepresentation.
		# 7) Update Cluster Sequence (Optional)
		if updateClusterSequence:
			self.originalClusterSequence = copy(self.clusterSequence) #So sloppy.
			self.mergeClusters_updateClusterSequence(c1, c2)
		return True

	# This updates the mergeCostReductions after mergeCluster1 and mergeCluster2 are merged.
	# This relies on NEW (i.e. post-merge) NGram Counts
	# That means mc2 shouldn't be in any tables anymore
	def mergeClusters_changeMergeCostReductions_MergedClusters(self, mc1):
		for c3 in self.clusters:
			if c3 == mc1:
				continue
			else:
				newMergeCostReduction = self.mergeCost_SinglePair(mc1, c3)
				self.mergeCostReductions[self.clusterCost_getClusterCostBigram(mc1, c3)] = newMergeCostReduction

	def mergeClusters_changeMergeCostReductions_RemoveDeletedCluster(self, mc2):
		for c3 in self.clusters:
			if c3 == mc2:
				continue
			else:
				del self.mergeCostReductions[self.clusterCost_getClusterCostBigram(mc2, c3)]

	# This updates the mergeCostReductions after mergeCluster1 and mergeCluster2 are merged.
	# This relies on OLD (i.e. pre-merge) NGram Counts
	def mergeClusters_changeMergeCostReductions_UnmergedClusters(self, mc1, mc2):
		cnt_mc1 = self.clusterCount_getClusterCount(mc1)
		cnt_mc2 = self.clusterCount_getClusterCount(mc2)
		numClusters = len(self.clusters)
		for i in range(numClusters):
			c3 = self.clusters[i]
			if (c3 == mc1 or c3 == mc2):
				continue
			cnt_c3 = self.clusterCount_getClusterCount(c3)
			for j in range(i):
				c4 = self.clusters[j]
				if (c4 == mc1 or c4 == mc2):
					continue
				cnt_c4 = self.clusterCount_getClusterCount(c4)
				mergeCostAddition = 0
				mergeCostAddition += self.clusterCost_getClusterCost(c3, mc1) # c3<->mc1 
				mergeCostAddition += self.clusterCost_getClusterCost(c3, mc2) # c3<->mc2 
				mergeCostAddition += self.clusterCost_getClusterCost(c4, mc1) # c4<->mc1 
				mergeCostAddition += self.clusterCost_getClusterCost(c4, mc2) # c4<->mc2
				mergeCostAddition += self.mutualInfo_PairVersusPair(c3, c4, mc1, mc2, cnt_c3, cnt_c4, cnt_mc1, cnt_mc2) # (c3+c4)<->(mc1+mc2)
				mergeCostReduction = 0
				mergeCostReduction += self.mutualInfo_PairVersusAnother(mc1, mc2, c3, cnt_mc1, cnt_mc2, cnt_c3) # c3<->(mc1+mc2)
				mergeCostReduction += self.mutualInfo_PairVersusAnother(mc1, mc2, c4, cnt_mc1, cnt_mc2, cnt_c4) # c4<->(mc1+mc2)
				mergeCostReduction += self.mutualInfo_PairVersusAnother(c3, c4, mc1, cnt_c3, cnt_c4, cnt_mc1) # mc1<->(c3+c4)
				mergeCostReduction += self.mutualInfo_PairVersusAnother(c3, c4, mc2, cnt_c3, cnt_c4, cnt_mc2) # mc2<->(c3+c4)
				mergeCostChange = (mergeCostAddition - mergeCostReduction)
				self.mergeCostReductions[self.clusterCost_getClusterCostBigram(c3, c4)] += mergeCostChange

	def performClustering_convertMergeHistoryToBinaryWords(self):
		self.binaryRepresentation = dict.fromkeys(self.originalClusters, '')
		for (mc1, mc2) in reversed(self.mergeHistory):
			self.binaryRepresentation[mc2] = self.binaryRepresentation[mc1] + '1'
			self.binaryRepresentation[mc1] = self.binaryRepresentation[mc1] + '0'
		maxDepth = 0
		for (cluster, binRep) in self.binaryRepresentation.iteritems():
			maxDepth = max(maxDepth,len(binRep))
		self.binaryRepresentationMaxLen = maxDepth


	def performClustering_convertMergeHistoryToDictionary(self):
		clusterWordMapping = dict(zip(self.wordClusterMapping.values(),self.wordClusterMapping.keys()))
		d = {}
		for (mc1, mc2) in self.mergeHistory:
			leftNode = d.get(mc1, (mc1, clusterWordMapping[mc1]))
			rightNode = d.get(mc2, (mc2, clusterWordMapping[mc2]))
			d[mc1] = {'l':leftNode,'r':rightNode}
		self.dictionaryRepresentation = {'root': d[mc1]}

	# Convert History to D3-readable JSON Representation
	def performClustering_convertMergeHistoryToJSON_Recursive(self, node=None, depth=0):
		if 'l' in node:
			leftNode = self.performClustering_convertMergeHistoryToJSON_Recursive(node=node['l'], depth=depth+1)
			rightNode = self.performClustering_convertMergeHistoryToJSON_Recursive(node=node['r'], depth=depth+1)
			return {"name": "internal", "children": [leftNode, rightNode]}
		else:
			return {"name": str(node), "size":1}

	def performClustering_convertMergeHistoryToJSON(self, prettyPrint=False):
		if not hasattr(self, 'dictionaryRepresentation'): #slopy
			self.performClustering_convertMergeHistoryToDictionary() #sloppy.
		rootNode = self.dictionaryRepresentation['root']
		d = self.performClustering_convertMergeHistoryToJSON_Recursive(rootNode)
		if prettyPrint:
			jsonarray = json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))
		else:
			jsonarray = json.dumps(d)
		self.JSONRepresentation = jsonarray

	# Sloppy. Should Move Out
	def performClustering_writeJSONToFile(self, fileName='brownianCluster.json'):
		if not hasattr(self, 'JSONRepresentation'): #slopy
			self.performClustering_convertMergeHistoryToJSON() #sloppy.
		writer = open(fileName, 'wb')
		writer.write(self.JSONRepresentation)
		writer.close()

	def performClustering_findInterClusterDistances(self):
		if len(self.binaryRepresentation) == 0: #sloppy.
			self.performClustering_convertMergeHistoryToBinaryWords() #sloppy. Use task stack instead.
		self.interClusterDistance = {}
		numClusters = len(self.originalClusters)
		for i in range(numClusters):
			for j in range(i):
				matchLength = self.performClustering_findInterClusterDistances_stringMatchLength(self.binaryRepresentation[i],self.binaryRepresentation[j])
				self.interClusterDistance[(i,j)] = self.binaryRepresentationMaxLen - matchLength

	# Consider: could be done more efficiently (i.e. don't use strings)
	def performClustering_findInterClusterDistances_stringMatchLength(self, string1, string2):
		length = 0
		for i in range(min(len(string1), len(string2))):
			if string1[i] != string2[i]:
				break
			length += 1
		return length

	def performClustering_findDistanceTableEntry(self, coord1, coord2, sequenceLength=None):
		sequenceLength = self.sequenceLength if sequenceLength is None else sequenceLength
		# This is upper right triangle coordinates
		# That means Row, then Column. So if coord1 is 0, it is The First Row. 
		if coord1 == coord2:
			raise Exception("Distance from coordinate to self is 0.")
		# Swap Coord1 and Coord2 if necessary
		if coord1 > coord2:
			temp = coord2
			coord2 = coord1
			coord1 = temp
		# Calculate table distance as if table were whole.
		tableEntry = coord1*sequenceLength + coord2
		# Subtract lower left triangle
		tableEntry -= ((coord1+1)*(coord1+2)/2)
		return tableEntry

	# TODO: Organize this better
	def performClustering_printDistanceTable(self, displayCount=10):
		if displayCount > self.sequenceLength:
			raise Exception('Attempting to print more than table contains.')
		for row in range(displayCount):
			blankEntries = ['x']*(row+1)
			rowEntries = []
			for col in range(row+1,displayCount):
				rowEntries.append(self.distanceTable[self.performClustering_findDistanceTableEntry(row, col)])
			print '\t'.join(map(str,blankEntries)),
			print '\t',
			print '\t'.join(map(str,rowEntries))

	def getinterClusterDistanceBigram(self, c1, c2):
		# Assumes one of the two is in the table...
		return (c1, c2) if (c1, c2) in self.interClusterDistance else (c2, c1)

	def getinterClusterDistance(self, c1, c2):
		return self.interClusterDistance[self.getinterClusterDistanceBigram(c1, c2)]

	# This could be done *WAY* more efficiently
	def performClustering_establishDistanceMeasure(self): 
		if hasattr(self, 'originalClusterSequence'): #sloppy
			seq = self.originalClusterSequence #sloppy
		else: #sloppy
			seq = self.clusterSequence #sloppy
		distanceTableSize = (self.sequenceLength * (self.sequenceLength-1))/2
		self.distanceTable = np.empty(distanceTableSize) #careful. This could blow up
		for i in range(self.sequenceLength):
			c1 = seq[i]
			for j in range(i+1,self.sequenceLength):
				c2 = seq[j]
				entry = self.performClustering_findDistanceTableEntry(i,j)
				if c1 == c2:
					self.distanceTable[entry] = 0
				else:	
					self.distanceTable[entry] = self.getinterClusterDistance(c1,c2)

	def performBrownianClustering(self):
		leftToMerge=True
		while leftToMerge:
			leftToMerge = self.mergeClusters_mergeTop()
		self.performClustering_convertMergeHistoryToBinaryWords()
		self.performClustering_findInterClusterDistances()
		self.performClustering_establishDistanceMeasure()


	def mergeClusters_updateClusterSequence(self, mc1, mc2):
		for i in range(self.sequenceLength):
			if self.clusterSequence[i] == mc2:
				self.clusterSequence[i] = mc1

	def findTotalClusteringCost(self):
		qualityCost = 0
		for c1 in self.clusters:
			cnt_c1 = self.clusterCount_getClusterCount(c1)
			for c2 in self.clusters:
				cnt_c2 = self.clusterCount_getClusterCount(c2)
				bigramCount = self.clusterCount_getBigramCount(c1, c2)
				qualityCost += self.mutualInfo_Equation(bigramCount, cnt_c1, cnt_c2)
		return qualityCost

	# def findBrownClustering(self, numInitialClusters=100):
	# This will be implemented when we have clusters folded in one at a time.
	# 	raise NotImplementedError
	# 	self.resetData_defineWordClusterMap(self, numInitialClusters=numInitialClusters)
	# 	clusterSequence = self.resetData_defineClusterSequence()
	# 	clusterNGramCounts = self.resetData_defineClusterCounts(clusterSequence)
	# 	nextWordPointer = numInitialClusters
	# 	unclusteredWords = self.NO_CLUSTER_SYMBOL in clusterNGramCounts[0]
	# 	unmergedClusters = len(clusterNGramCounts) > 1
	# 	while unclusteredWords or unmergedClusters:
	# 		if unclusteredWords:
	# 			changeWord = self.sortedWords[nextWordPointer]
	# 			(clusterSequence, clusterNGramCounts) = self.changeWordCluster(	clusterSequence,
	# 																			clusterNGramCounts,
	# 																			changeWord,
	# 																			nextWordPointer,
	# 																			oldCluster=self.NO_CLUSTER_SYMBOL)
	# 			nextWordPointer += 1
	# 			unclusteredWords = self.NO_CLUSTER_SYMBOL in clusterNGramCounts[0]
	# 		if unmergedClusters:
	# 			raise NotImplementedError
	# 			unmergedClusters = len(clusterNGramCounts) > 1
