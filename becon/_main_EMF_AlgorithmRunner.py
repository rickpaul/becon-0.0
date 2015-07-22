import numpy as np
import sqlite3 as sq
import logging as log

from os.path import isfile
from random import sample

import EMF_DatabaseHelper as EM_DBHelp
import EMF_util as EM_util

import cluster_BrownianCluster as cluster_Brown


def performInitialSetup(DBFilePath=None, recordLog=False):
	# Establish Global Variables
	global db_cursor
	global db_connection

	EM_util.initializeLog(recordLog=recordLog)

	if DBFilePath is None:
		DBFilePath = EM_util.defaultDB
	
	log.info('Connecting to Database: \n%s', DBFilePath)
	
	# Create Database Connection
	db_connection = sq.connect(DBFilePath)
	db_cursor = db_connection.cursor()
	log.info('Database opened successfully')

def sampleWordIDs(numSamples=1, wordLength=5):
	#TODO: Implement limits:
	# 1) data have same periodicity
	# 2) data come from certain supertype
	# 2) data come from certain subtype
	# 2) data come from certain type
	# 2) data come from certain data_master_ids
	# 2) data come from differing data_master_ids (i.e. no way to have same data_master_id)
	allIDs = EM_DBHelp.retrieve_AllWordSeriesIDs(db_connection, db_cursor)
	words = []
	for i in range(numSamples):
		words.append(sample(allIDs, wordLength))

	wordSeriesIDs = {ID for word in words for ID in word}

	getFromDB = lambda (seriesID): \
	EM_DBHelp.retrieve_WordSeriesMetaData(db_connection, db_cursor, columnName, seriesID=seriesID)

	columnName = 'dt_latest_word'
	dates = [getFromDB(ID) for ID in wordSeriesIDs]
	latestDate = min(dates)

	columnName = 'dt_earliest_word'
	dates = [getFromDB(ID) for ID in wordSeriesIDs]
	earliestDate = max(dates)
	
	columnName = 'int_data_master_id'
	dataSeriesIDs = [getFromDB(ID) for ID in wordSeriesIDs]

	return (words, wordSeriesIDs, dataSeriesIDs, earliestDate, latestDate)

def generateWordsFromIndices(wordSeriesIDs, earliestDate, latestDate):
	wordSize = len(wordSeriesIDs)
	# Get Data Series
	words = None
	dates = None
	for i in range(wordSize):
		data = EM_DBHelp.getWordHistory_WordHistoryTable(	db_connection, db_cursor, 
															wordSeriesIDs[i], 
															beforeDate=latestDate, afterDate=earliestDate)
		dt = np.dtype('int,float')
		data = np.array(data, dtype=dt)
		data.dtype.names = ['dates','values']
		if words is None:
			words = np.empty((len(data), wordSize))
			dates = data['dates']
		if not (data['dates']==dates).all():
			raise Exception('Dates for different series do not match up')
		words[:,i] = data['values']
	# Tuplefy words
	numWords = words.shape[0]
	for j in range(numWords):
		words[j,:] = tuple(words[j,:])
	# Return
	return (dates, words)

def createBrownianDistanceMetric(numPasses=50):
	(wordIDCollections, wordSeriesIDs, dataSeriesIDs, earliestDate, latestDate) = \
	sampleWordIDs(numSamples=numPasses, wordLength=2)

	dates = None
	distances = None
	for wordIDCollection in wordIDCollections:
		(wordDates, words) = generateWordsFromIndices(wordIDCollection, earliestDate, latestDate)
		if dates is None:
			dates = wordDates
		if not (dates == wordDates).all():
			raise Exception('Dates for different series do not match up')
		cB = cluster_Brown.BrownClusterModel()
		cB.addTrainingData(words)
		cB.performBrownianClustering()
		cB.performClustering_printDistanceTable()
		if distances is None:
			distances = cB.distanceTable
		else:
			distances += cB.distanceTable
	distances = distances/(numPasses*1.0)
	return distances




def finalize():
	db_connection.close()

if __name__ == '__main__':
	try:
		performInitialSetup()
		distances = createBrownianDistanceMetric(numPasses=5)
	except:
		raise
	finally:
		finalize()