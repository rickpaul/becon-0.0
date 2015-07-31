import datetime 

import numpy as np
import sqlite3 as sq
import logging as log

from json import dumps as json_dump
from json import loads as json_load
from os.path import isfile

import EMF_DatabaseHelper as EM_DBHelp
import EMF_DataGenerator_util as EM_DGUtil
import EMF_util as EM_util


def performInitialSetup(DBFilePath=None, fileFormat='rickshawJSON'):
	# Establish Global Variables
	global db_curs
	global db_conn
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
	db_conn = sq.connect(DBFilePath)
	db_curs = db_conn.cursor()
	log.info('Database opened successfully')
	
def convertDataHistoryToJSON(ticker, dataSeriesID=None):
	global file_format

	if dataSeriesID is None:
		dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	db_conn, db_curs, 
														dataTicker=ticker, 
														insertIfNot=False)

	dataSeries = EM_DBHelp.getCompleteDataHistory_DataHistoryTable(	db_conn, db_curs, dataSeriesID)

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
	dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	db_conn, db_curs, 
													dataTicker=ticker, 
													insertIfNot=False)
	
	data_JSON = convertDataHistoryToJSON(ticker, dataSeriesID=dataSeriesID)

	columnName = 'dt_earliest_value'
	earliestData = EM_DGUtil.getFromDB()
	columnName = 'dt_latest_value'
	latestData = EM_DGUtil.getFromDB()
	columnName = 'dt_last_updated_history'
	lastUpdate = EM_DGUtil.getFromDB()

	writeFile = generateDateHistoryFilename(ticker, earliestData, latestData, lastUpdate)
	updateAvailableSeriesDataFile(ticker, earliestData, latestData, lastUpdate, dataFileName=writeFile)

	try:
		writer = open(writeFile, 'wb')
		writer.write(json_dump(data_JSON))
	except:
		raise
	finally:
		writer.close()

def generateWords_FirstOrderDiff_SingleTicker(ticker):
	wordType = EM_util.wordTypes['1OD']
	wordSuperType = EM_util.wordSuperTypes['generic']

	dg_handle = EM_DGUtil.EMF_DataGenerator_Handle(db_conn, db_curs)

	dg_handle.findAndStoreDataSeries(ticker)

	if dg_handle.dataPeriodicity == 365: #Daily
		raise NotImplementedError
	elif dg_handle.dataPeriodicity == 52: #Weekly
		raise NotImplementedError
	elif dg_handle.dataPeriodicity == 12: #Monthly
		periods = [1, 3, 6, 12, 18, 24, 36, 48, 60]
		periodDesc = 'M'
	elif dg_handle.dataPeriodicity == 4: #Quarterly
		periods = [1, 2, 3, 4, 6, 8, 12 ,16, 20]
		periodDesc = 'Q'
	else:
		raise Exception('dataPeriodicity not recognized.')

	if len(EM_DGUtil.firstOrderDiffPeriodOverride) > 0:
		periods = EM_DGUtil.firstOrderDiffPeriodOverride

	dataSeries = dg_handle.getDataHistory()

	for periodLength in periods:
		wordTicker = '1OD|' + str(periodLength) + periodDesc + '|' + ticker
		wordSubType = periodLength
		
		dg_handle.findAndStoreWordSeries(wordTicker, wordSubType, wordType, wordSuperType)

		firstOrderDiffData = EM_DGUtil.findFirstOrderDifferences(dataSeries)

		dates = []
		values = []

		for i in range(len(firstOrderDiffData)):
			dates.append(dataSeries['dates'][i+periodLength])
			values.append(firstOrderDiffData[i])

		dg_handle.insertWords(dates, values)
		dg_handle.resetWordSeries()

def generateWords_TimeToSincePeakTrough():
	tickers = ['USRECM', 'USARECM']

def generateWords_TimeToSincePeakTrough_SingleTicker(ticker):
	wordType = EM_util.wordTypes['timeToRecession']
	wordSuperType = EM_util.wordSuperTypes['recessionSignal']
	wordSubType = 1
	wordTicker = 'TimeToRecession|' + ticker

	dg_handle = EM_DGUtil.EMF_DataGenerator_Handle(db_conn, db_curs)

	dg_handle.findAndStoreDataSeries(ticker)
	dg_handle.findAndStoreWordSeries(wordTicker, wordSubType, wordType, wordSuperType)

	if EM_DGUtil.dataPeriodicity != 12:
		raise Exception('Not yet built to handle non-monthly data')

	dg_handle.getDataHistory(dataType='int')

	since_dates = []
	since_values = []

	to_dates = []
	to_values = []

	currentValue = dataSeries['values'][0]
	i = 0
	while i < len(dataSeries):
		count = 1
		while dataSeries['values'][i] == currentValue:
			count += 1
			i += 1
		currentValue = dataSeries['values'][i]
		for j in count:
			date = dataSeries['dates'][i-j]
			value = j

	dg_handle.resetWordSeries()


def finalize():
	db_conn.close()

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