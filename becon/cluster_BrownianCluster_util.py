import json

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

def writeJSON(dataDictionary, fileName, prettyPrint=False):
	# Print the data to JSON
	if prettyPrint:
		JSONRepresentation = json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))
	else:
		JSONRepresentation = json.dumps(d)	
	# Write the data
	writer = open(fileName, 'wb')
	writer.write(JSONRepresentation)
	writer.close()

def createDrawFriendlyDictionary(clusterSequence, wordSequence, rawData=None dates=None):
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

