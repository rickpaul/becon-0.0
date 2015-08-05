import pudb #TEST
import json
from matplotlib import pyplot as plt
from matplotlib import patches as patches

from sys import maxint

from cluster_BrownianCluster import BrownClusterModel

LEFT_NODE = -2
RIGHT_NODE = -1

# Convert History to D3-readable JSON Representation
def __convertMergeHistoryToJSON_Recursive(node=None, depth=0):
	if LEFT_NODE in node: # if not leaf
		leftNode = convertMergeHistoryToJSON_Recursive(node=node[LEFT_NODE], depth=depth+1)
		rightNode = convertMergeHistoryToJSON_Recursive(node=node[RIGHT_NODE], depth=depth+1)
		return {"name": "internal", "children": [leftNode, rightNode]}
	else:
		return {"name": str(node), "size":1}

def convertMergeHistoryToJSON(dictionaryRepresentation, prettyPrint=False, fileName='brownianCluster.json'):
	# Convert the data to D3-friendly representation
	rootNode = dictionaryRepresentation['root']
	d = __convertMergeHistoryToJSON_Recursive(rootNode)
	return d

def writeDictionaryToJSON(dataDictionary, fileName, prettyPrint=False):
	# Print the data to JSON
	if prettyPrint:
		JSONRepresentation = json.dumps(dataDictionary, sort_keys=True, indent=4, separators=(',', ': '))
	else:
		JSONRepresentation = json.dumps(dataDictionary)
	# Write the data
	writer = open(fileName, 'wb')
	writer.write(JSONRepresentation)
	writer.close()

class DrawHandler_BCM:
	SEQUENCE_CLUSTER_NUM_KEY = 'cl'
	SEQUENCE_DATA_KEY = 'dat'
	SEQUENCE_WORD_KEY = 'wd'

	def __init__(self, addTestData=True):
		self.BCM = BrownClusterModel()
		if addTestData:
			self.addTestData_1OD()
	
	def addTestData_1OD(self, numAssets=3, dataLength=200, differencePeriod=10, addZeroPrefix=True):
		from EMF_DataGenerator_util import findFirstOrderDifferences
		from stats_util import CorrelatedReturnHistoryGenerator
		
		self.rawTestData = CorrelatedReturnHistoryGenerator(numAssets=numAssets, 
															dataLength=dataLength,
															addZeroPrefix=addZeroPrefix).history
		self.wordTestData = findFirstOrderDifferences(self.rawTestData, differencePeriod)
		
		self.BCM.addTrainingData(self.wordTestData)

		self.addZeroPrefix = addZeroPrefix
		self.differencePeriod = differencePeriod

	def createDrawFriendlyDictionary_10D(self, dataSequence, clusterSequence, wordSequence, differencePeriod):
		# Get Sequence Data
		(dataLength, dataWidth) = dataSequence.shape
		(wordLength, wordWidth) = wordSequence.shape
		# Verify Data Shapes
		if type(clusterSequence) == list:
			assert len(clusterSequence) == wordLength
		else:
			assert clusterSequence.shape == (wordLength, 1)
			clusterSequence = clusterSequence[:,0]
		assert wordLength + differencePeriod == dataLength

		# Create Sequence Data
		wordMin = dataMin = maxint
		wordMax = dataMax = -maxint - 1
		sequenceData = []
		for i in range(dataLength):
			d = {}
			d[self.SEQUENCE_DATA_KEY] = tuple(dataSequence[i,:])
			dataMin = min(dataMin, min(dataSequence[i,:]))
			dataMax = max(dataMax, max(dataSequence[i,:]))
			if i >= differencePeriod:
				j = i - differencePeriod
				d[self.SEQUENCE_CLUSTER_NUM_KEY] = clusterSequence[j]
				d[self.SEQUENCE_WORD_KEY] = tuple(wordSequence[j,:])
				wordMin = min(wordMin, min(wordSequence[j,:]))
				wordMax = max(wordMax, max(wordSequence[j,:]))
			else:
				d[self.SEQUENCE_CLUSTER_NUM_KEY] = None
				d[self.SEQUENCE_WORD_KEY] = None

			sequenceData.append(d)

		# Write Data to Dictionary
		drawDict = {}

		drawDict['hasDate'] = False
		
		drawDict['sequenceData'] = sequenceData

		drawDict['differencePeriod'] = differencePeriod

		drawDict['dataStart'] = 0
		drawDict['dataEnd'] = dataLength
		drawDict['dataWidth'] = dataWidth
		drawDict['dataMin'] = dataMin # Deprecable, but makes json smoother
		drawDict['dataMax'] = dataMax # Deprecable, but makes json smoother

		drawDict['wordStart'] = differencePeriod
		drawDict['wordEnd'] = drawDict['dataEnd']
		drawDict['wordWidth'] = wordWidth
		drawDict['wordMin'] = wordMin # Deprecable, but makes json smoother
		drawDict['wordMax'] = wordMax # Deprecable, but makes json smoother

		return drawDict

	def __wordDisplayPad(self, value):
		return value*1.2

	def getSequenceData_DrawFriendlyDictionary_1OD(self, drawDict, key, column):
		sequenceData = drawDict['sequenceData']
		if key == self.SEQUENCE_DATA_KEY:
			start = drawDict['dataStart']
			end = drawDict['dataEnd']
			return [d[key][column] for d in sequenceData[start:end]]
		elif key == self.SEQUENCE_WORD_KEY:
			start = drawDict['wordStart']
			end = drawDict['wordEnd']
			return [d[key][column] for d in sequenceData[start:end]]
		elif key == self.SEQUENCE_CLUSTER_NUM_KEY:
			start = drawDict['wordStart']
			end = drawDict['wordEnd']
			return [d[key] for d in sequenceData[start:end]]
		else:
			raise Exception('Key Type not recognized')

	def drawBCMUsingPyPlot(self, numberOfLoops=None):
		numClusters = len(self.BCM.clusters)
		self.colors = self.drawBCMUsingPyPlot_get_N_HexColors(numClusters)
		moreToMerge = True
		count = 0
		while moreToMerge:
			moreToMerge = self.BCM.mergeClusters_mergeTop(updateClusterSequence=True)
			# TODO: In future, create methods to only update clusterSequence
			# 		Do we want DrawDict to be an object eventually?
			drawDictionary = self.createDrawFriendlyDictionary_10D(	self.rawTestData, 
																	self.BCM.clusterSequence, 
																	self.wordTestData, 
																	self.differencePeriod)
			self.drawBCMUsingPyPlot_singleDrawing(drawDictionary)
			count += 1
			if numberOfLoops is not None and count >= numberOfLoops:
				return drawDictionary

	def drawBCMUsingPyPlot_singleDrawing(self, drawDict):
		plotStart = min(drawDict['dataStart'], drawDict['wordStart'])
		plotEnd = max(drawDict['dataEnd'], drawDict['wordEnd'])

		plt.figure(1)
		# Draw Raw Data
		plt.subplot(211)
		plt.xlim(plotStart, plotEnd)
		xValsData = range(drawDict['dataStart'], drawDict['dataEnd'])
		for data_i in range(drawDict['dataWidth']):
			# pu.db #TEST
			plt.plot(	xValsData, 
						self.getSequenceData_DrawFriendlyDictionary_1OD(drawDict, self.SEQUENCE_DATA_KEY, data_i))

		# Draw Words
		subplot = plt.subplot(212)
		plt.xlim(plotStart, plotEnd)
		plt.ylim(self.__wordDisplayPad(drawDict['wordMin']), self.__wordDisplayPad(drawDict['wordMax']))
		xVals_Words = range(drawDict['wordStart'], drawDict['wordEnd'])
		for word_i in range(drawDict['wordWidth']):
			plt.plot(	xVals_Words, 
						self.getSequenceData_DrawFriendlyDictionary_1OD(drawDict, self.SEQUENCE_WORD_KEY, word_i))
		self.drawBCMUsingPyPlot_drawClusterPatches(subplot, drawDict)
		# Show Drawing
		plt.show()

	def drawBCMUsingPyPlot_drawClusterPatches(self, subplot, drawDict):
		lowY = self.__wordDisplayPad(drawDict['wordMin'])
		height = self.__wordDisplayPad(drawDict['wordMax']) - self.__wordDisplayPad(drawDict['wordMin'])
		width = 1.0
		for i in range(drawDict['wordStart'], drawDict['wordEnd']):
			subplot.add_patch(
				patches.Rectangle(
					(i, lowY), width, height,
					alpha=0.5,
					facecolor=self.colors[self.BCM.clusterSequence[i-drawDict['differencePeriod']]],
					edgecolor='none'
			))

	# Taken from online (source forgotten...)
	def drawBCMUsingPyPlot_get_N_HexColors(self, N=5):
		from colorsys import hsv_to_rgb
		HSV_tuples = [(x*1.0/N, 0.5, 0.5) for x in xrange(N)]
		hex_out = []
		for rgb in HSV_tuples:
			rgb = map(lambda x: int(x*255), hsv_to_rgb(*rgb))
			hexString = "".join(map(lambda x: chr(x).encode('hex'),rgb))
			hex_out.append('#'+hexString)
		return hex_out
