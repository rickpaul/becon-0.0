import json
from matplotlib import pyplot as plt
from matplotlib import patches as patches

from cluster_BrownianCluster import BrownClusterModel

LEFT_NODE = -2
RIGHT_NODE = -1
WORD_LENGTH_KEY = 'wL'
WORD_WIDTH_KEY = 'wW'
SEQUENCE_KEY = 'Sq'
SEQUENCE_CLUSTER_NUM_KEY = 'cl'
SEQUENCE_DATE_KEY = 'dt'
SEQUENCE_WORD_KEY = 'wd'

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
		JSONRepresentation = json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))
	else:
		JSONRepresentation = json.dumps(d)	
	# Write the data
	writer = open(fileName, 'wb')
	writer.write(JSONRepresentation)
	writer.close()

def createDrawFriendlyDictionary(clusterSequence, wordSequence, rawData=None, dates=None):
	# Get Sequence Data
	(wordLength, wordWidth) = wordSequence.shape
	assert len(clusterSequence) == wordLength

	hasRawData = (rawData is not None)
	if hasRawData:
		(dataLength, dataWidth) = rawData.shape

	# Create Dates
	hasDates = (dates is not None)
	if hasDates:
		assert len(dates) == dataLength
	else:
		dates = range(wordLength)

	# Create Sequence
	writeSequence = [
		{
			SEQUENCE_CLUSTER_NUM_KEY: c,
			SEQUENCE_DATE_KEY: d,
			SEQUENCE_WORD_KEY: w,
		}
		for (w,d,c) in zip (wordSequence, dates, clusterSequence)
	]

	# Write Data to Dictionary
	writeData = {}
	writeData[WORD_LENGTH_KEY] = wordLength
	writeData[WORD_WIDTH_KEY] = wordWidth
	writeData[SEQUENCE_KEY] = writeSequence

	return writeData

class PyPlotDrawHandler_BCM:
	def __init__(self, addTestData=True):
		self.BCM = BrownClusterModel()
		if addTestData:
			self.add1ODTestData()
	
	def add1ODTestData(self, numAssets=3, dataLength=200, differencePeriod=10):
		from EMF_DataGenerator_util import findFirstOrderDifferences
		from stats_util import CorrelatedReturnHistoryGenerator
		addZeroPrefix = 1
		self.rawTestData = CorrelatedReturnHistoryGenerator(numAssets=numAssets, 
															dataLength=dataLength,
															addZeroPrefix=addZeroPrefix).history
		self.wordTestData = findFirstOrderDifferences(self.rawTestData, differencePeriod)
		
		self.BCM.addTrainingData(self.wordTestData)

		self.differencePeriod = differencePeriod

		self.dataWidth = numAssets
		self.dataStart = 0
		self.dataEnd = dataLength + addZeroPrefix
		self.dataLength = self.dataEnd - self.dataStart
		
		self.wordWidth = numAssets
		self.wordStart = self.differencePeriod
		self.wordEnd = self.dataLength
		self.wordLength = self.wordEnd - self.wordStart
		self.wordMinValue = -1.0
		self.wordDisplayMin = self.wordMinValue * 1.2
		self.wordMaxValue = 1.0
		self.wordDisplayMax = self.wordMaxValue * 1.2

	def drawMergeSequenceUsingPyPlot(self):
		numClusters = len(self.BCM.clusters)
		self.colors = self.__get_N_HexColors(numClusters)
		moreToMerge = True
		while moreToMerge:
			self.drawBCMUsingPyPlot()
			moreToMerge = self.BCM.mergeClusters_mergeTop(updateClusterSequence=True)

	def drawBCMUsingPyPlot(self):
		plotStart = min(self.dataStart, self.wordStart)
		plotEnd = max(self.dataEnd, self.wordEnd)

		plt.figure(1)
		# Draw Raw Data
		plt.subplot(211)
		plt.xlim(plotStart, plotEnd)
		xValsData = range(self.dataStart, self.dataEnd)
		for asset in range(self.dataWidth):
			plt.plot(	xValsData, 
						self.rawTestData[self.dataStart:self.dataEnd, asset])

		# Draw Words
		subplot = plt.subplot(212)
		plt.xlim(plotStart, plotEnd)
		plt.ylim(self.wordDisplayMin, self.wordDisplayMax)
		xVals_Words = range(self.wordStart, self.wordEnd)
		for word in range(self.wordWidth):
			plt.plot(	xVals_Words, 
						self.wordTestData[:, word])
		self.drawMergeSequenceUsingPyPlot__drawPatches(subplot)
		# Show Drawing
		plt.show()

	def drawMergeSequenceUsingPyPlot__drawPatches(self, subplot):
		lowY = self.wordDisplayMin
		height = self.wordDisplayMax - self.wordDisplayMin
		width = 1.0
		for i in range(self.wordStart, self.wordEnd):
			subplot.add_patch(
				patches.Rectangle(
					(i, lowY), width, height,
					alpha=0.5,
					facecolor=self.colors[self.BCM.clusterSequence[i-self.differencePeriod]],
					edgecolor='none'
			))

	# Taken from online (source forgotten...)
	def __get_N_HexColors(self, N=5):
		from colorsys import hsv_to_rgb
		HSV_tuples = [(x*1.0/N, 0.5, 0.5) for x in xrange(N)]
		hex_out = []
		for rgb in HSV_tuples:
			rgb = map(lambda x: int(x*255), hsv_to_rgb(*rgb))
			hexString = "".join(map(lambda x: chr(x).encode('hex'),rgb))
			hex_out.append('#'+hexString)
		return hex_out
