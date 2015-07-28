import datetime 

import numpy as np
import sqlite3 as sq
import logging as log
from json import dumps as json_dump
from json import loads as json_load
from os.path import isfile

import EMF_DatabaseHelper as EM_DBHelp
import EMF_util as EM_util

def performInitialSetup(DBFilePath=None, fileFormat='rickshawJSON'):
	# Establish Global Variables
	global db_cursor
	global db_connection
	global file_format

	file_format = EM_util.rawSeriesFormatTypes[fileFormat]

	EM_util.initializeLog()

	if DBFilePath is None:
		DBFilePath = EM_util.defaultDB
	
	log.info('Connecting to Database: \n%s', DBFilePath)
	
	# Create Database
	if not isfile(DBFilePath):
		raise Exception('Database not found.')

	# Create Database Connection
	db_connection = sq.connect(DBFilePath)
	db_cursor = db_connection.cursor()
	log.info('Database opened successfully')
	
def convertDataHistoryToJSON(ticker, dataSeriesID=None):
	global file_format

	if dataSeriesID is None:
		dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	db_connection, db_cursor, 
														dataTicker=ticker, 
														insertIfNot=False)

	dataSeries = EM_DBHelp.getCompleteDataHistory_DataHistoryTable(	db_connection, db_cursor, dataSeriesID)

	if file_format[0] == 'rs':
		convertValue = lambda (dt, vl): {'x': dt, 'y': float(vl)}
	elif file_format[0] == 'd3':
		convertValue = lambda (dt, vl): {'date': dtConvert_EpochtoY_M_D(dt), 'value': float(vl)}
	else:
		raise Exception('File Format Type not recognized for date/value JSON')
	return map(convertValue, dataSeries)

def generateDateHistoryFilename(ticker, earliestData, latestData, lastUpdate):
	global file_format

	fileName = EM_util.webJSONRepository
	fileName += file_format[0] + '|'
	fileName += ticker + '|'
	fileName += str(earliestData) + '|'
	fileName += str(latestData) + '|'
	fileName += str(lastUpdate)
	fileName += file_format[1]
	return fileName

def readAvailableDataFile():
	try:
		reader = open(EM_util.webJSONAvailableDataFile, 'rb')
		data = reader.read()
	except:
		raise
	finally:
		reader.close()
	return json_load(data)

def getAvailableSeries():
	availableData = readAvailableDataFile()
	return availableData.keys()

def updateAvailableSeriesDataFile(ticker, earliestData, latestData, lastUpdate, dataFileName=None):
	global file_format

	availableData = readAvailableDataFile()

	if dataFileName is None:
		dataFileName = generateDateHistoryFilename(ticker, earliestData, latestData, lastUpdate)

	availableData[ticker] = {
		'ticker': ticker,
		'earliestData': earliestData,
		'latestData': latestData,
		'lastUpdated': lastUpdate,
		'format': file_format,
		'dataFileName': dataFileName
	}

	try:
		writer = open(AVAILABLE_DATA_JSON_FILE, 'wb')
		data = writer.write(json_dump(availableData))
	except:
		raise
	finally:
		writer.close()

def writeDataHistorytoJSON(ticker):
	dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	db_connection, db_cursor, 
													dataTicker=ticker, 
													insertIfNot=False)
	
	data_JSON = convertDataHistoryToJSON(ticker, dataSeriesID=dataSeriesID)

	getFromDB = lambda: \
	EM_DBHelp.retrieve_DataSeriesMetaData(db_connection, db_cursor, columnName, seriesID=dataSeriesID)

	columnName = 'dt_earliest_value'
	earliestData = getFromDB()
	columnName = 'dt_latest_value'
	latestData = getFromDB()
	columnName = 'dt_last_updated_history'
	lastUpdate = getFromDB()

	writeFile = generateDateHistoryFilename(ticker, earliestData, latestData, lastUpdate)
	updateAvailableSeriesDataFile(ticker, earliestData, latestData, lastUpdate, dataFileName=writeFile)

	try:
		writer = open(writeFile, 'wb')
		writer.write(json_dump(data_JSON))
	except:
		raise
	finally:
		writer.close()

def findNormalDistRange(data):
	func_round = np.vectorize(round)
	mean = np.mean(data)
	stds = np.std(data)
	stdVols = (data - mean)/stds
	return func_round(stdVols - 0.5*np.sign(stdVols))

def generateFirstOrderDifferenceWords(ticker, dataPeriodicity=None, periods=[]):
	wordType = EM_util.wordTypes['1OD']
	wordSuperType = EM_util.wordSuperTypes['generic']

	dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	db_connection, db_cursor, 
													dataTicker=ticker, 
													insertIfNot=False)

	getFromDB = lambda: \
	EM_DBHelp.retrieve_DataSeriesMetaData(db_connection, db_cursor, columnName, seriesID=dataSeriesID)

	if dataPeriodicity is None:
		columnName = 'int_data_periodicity'
		dataPeriodicity = getFromDB()

	if dataPeriodicity == 365: #Daily
		raise NotImplementedError
	elif dataPeriodicity == 52: #Weekly
		raise NotImplementedError
	elif dataPeriodicity == 12: #Monthly
		if len(periods) == 0:
			periods = [1, 3, 6, 12, 18, 24, 36, 48, 60]
			# periods = [1, 2, 3, 4, 5, 6, 9, 12, 18, 24, 30, 36, 48, 60]
		periodDesc = 'M'
	elif dataPeriodicity == 4: #Quarterly
		if len(periods) == 0:
			periods = [1, 2, 3, 4, 6, 8, 12 ,16, 20]
		periodDesc = 'Q'
	else:
		raise Exception('dataPeriodicity not recognized.')

	dataSeries = EM_DBHelp.getCompleteDataHistory_DataHistoryTable(db_connection, db_cursor, dataSeriesID)
	dt = np.dtype('int,float')
	dataSeries = np.array(dataSeries, dtype=dt)
	dataSeries.dtype.names = ['dates','values']

	for periodLength in periods:
		key = '1OD|' + str(periodLength) + periodDesc + '|' + ticker
		wordSubType = periodLength
		wordSeriesID = EM_DBHelp.retrieve_WordSeriesID(	db_connection, db_cursor, 
														dataSeriesID, wordType, wordSubType, wordSuperType,
														wordTicker=key, 
														insertIfNot=True)
		firstOrderDiffData = dataSeries['values'][periodLength:] - dataSeries['values'][:-periodLength]
		firstOrderDiffData = findNormalDistRange(firstOrderDiffData)

		sendToDB = lambda: \
		EM_DBHelp.update_WordSeriesMetaData(db_connection, db_cursor, columnName, value, seriesID=wordSeriesID)		

		successfulInserts = 0
		unsuccessfulInserts = 0

		for i in range(len(firstOrderDiffData)):
			date = dataSeries['dates'][i+periodLength]
			value = firstOrderDiffData[i]
			(success, error) = EM_DBHelp.insertDataPoint_WordHistoryTable(	db_connection, db_cursor, 
																			wordSeriesID, 
																			date, 
																			value)
			if not success:
				log.warning('\t\tFailed to Write Historical Word at %s for %s [value = %f]', key, date, value)
				unsuccessfulInserts += 1
			else:
				successfulInserts +=1
	
		minDate = dataSeries['dates'][periodLength]
		maxDate = dataSeries['dates'][len(dataSeries)-1]

		log.info('\t%d Historical Words Written Successfuly for %s', successfulInserts, key)
		log.info('\t%d Historical Words Writing Failed for %s', unsuccessfulInserts, key)
		log.info('\tDate range from %s to %s Written for %s', EM_util.dtConvert_EpochtoY_M_D(minDate), EM_util.dtConvert_EpochtoY_M_D(maxDate), key)

		# Update Successful Inserts
		columnName = 'int_unsuccessful_generations'; value = unsuccessfulInserts; sendToDB()
		
		# Update Word Periodicity
		columnName = 'int_word_periodicity'; value = dataPeriodicity; sendToDB()

		# Update Earliest Word
		columnName = 'dt_earliest_word'; value = minDate; sendToDB()

		# Update Latest Word
		columnName = 'dt_latest_word'; value = maxDate; sendToDB()

		# Update Last Update Date
		columnName = 'dt_last_generated'; value = EM_util.dtGetNowAsEpoch(); sendToDB()

def generateTimeToRecessionStats():
	wordType = EM_util.wordTypes['timeToRecession']
	wordSuperType = EM_util.wordSuperTypes['recessionSignal']

	getFromDB = lambda: \
	EM_DBHelp.retrieve_DataSeriesMetaData(db_connection, db_cursor, columnName, seriesID=dataSeriesID)
	

	tickers = ['USRECM', 'USARECM']
	for index, ticker in enumerate(tickers):
		dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	db_connection, db_cursor, 
														dataTicker=ticker, 
														insertIfNot=False)
		columnName = 'int_data_periodicity'
		if getFromDB() != 12:
			raise Exception('Not yet built to handle non-monthly data')
		key = 'TimeToRecession|' + ticker
		wordSubType = index
		wordSeriesID = EM_DBHelp.retrieve_WordSeriesID(	db_connection, db_cursor, 
														dataSeriesID, wordType, wordSubType, wordSuperType,
														wordTicker=key, 
														insertIfNot=True)

		dataSeries = EM_DBHelp.getCompleteDataHistory_DataHistoryTable(db_connection, db_cursor, dataSeriesID)
		dt = np.dtype('int,int')
		dataSeries = np.array(dataSeries, dtype=dt)
		dataSeries.dtype.names = ['dates','values']

		isRecession = dataSeries['values'][0]
		i = 0
		while i < len(dataSeries):
			count = 1
			while dataSeries['values'][i] == isRecession:
				count += 1
				i += 1
			isRecession = dataSeries['values'][i]
			for j in count:
				date = dataSeries['values'][i-j]
				value = j
				(success, error) = EM_DBHelp.insertDataPoint_WordHistoryTable(	db_connection, db_cursor, 
																				wordSeriesID, 
																				date, 
																				value)
				if not success:
					log.warning('\t\tFailed to Write Historical Word at %s for %s [value = %f]', key, date, value)
					unsuccessfulInserts += 1
				else:
					successfulInserts +=1

		minDate = dataSeries['dates'][0]
		maxDate = dataSeries['dates'][len(dataSeries)-1]

		log.info('\t%d Historical Words Written Successfuly for %s', successfulInserts, key)
		log.info('\t%d Historical Words Writing Failed for %s', unsuccessfulInserts, key)
		log.info('\tDate range from %s to %s Written for %s', EM_util.dtConvert_EpochtoY_M_D(minDate), EM_util.dtConvert_EpochtoY_M_D(maxDate), key)

		# Update Successful Inserts
		columnName = 'int_unsuccessful_generations'; value = unsuccessfulInserts; sendToDB()
		
		# Update Word Periodicity
		columnName = 'int_word_periodicity'; value = dataPeriodicity; sendToDB()

		# Update Earliest Word
		columnName = 'dt_earliest_word'; value = minDate; sendToDB()

		# Update Latest Word
		columnName = 'dt_latest_word'; value = maxDate; sendToDB()

		# Update Last Update Date
		columnName = 'dt_last_generated'; value = EM_util.dtGetNowAsEpoch(); sendToDB()


def finalize():
	db_connection.close()

if __name__ == '__main__':
	try:
		performInitialSetup()
		generateTimeToRecessionStats()
		# generateFirstOrderDifferenceWords('AAA', dataPeriodicity=12, periods=[]) #Test
		# generateFirstOrderDifferenceWords('UMCSENT', dataPeriodicity=12, periods=[]) #Test
		# generateFirstOrderDifferenceWords('AAA', dataPeriodicity=12, periods=[60]) #Test
		# generateFirstOrderDifferenceWords('UMCSENT', dataPeriodicity=12, periods=[60]) #Test
	except:
		raise
	finally:
		finalize()