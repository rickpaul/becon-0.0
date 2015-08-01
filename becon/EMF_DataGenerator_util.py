from sys import maxint

import numpy as np
import logging as log

import EMF_DatabaseHelper as EM_DBHelp

firstOrderDiffPeriodOverride = []

def findFirstOrderDifferences(data):
	firstOrderDiffData = data[periodLength:] - data[:-periodLength]
	return findNormalDistRange(firstOrderDiffData)

def findNormalDistRange(data):
	func_round = np.vectorize(round)
	mean = np.mean(data)
	stds = np.std(data)
	stdVols = (data - mean)/stds
	return func_round(stdVols - 0.5*np.sign(stdVols))


getFromDB = lambda: EM_DBHelp.retrieve_DataSeriesMetaData(db_conn, db_curs, columnName, seriesID=dataSeriesID)
sendToDB = lambda: EM_DBHelp.update_WordSeriesMetaData(db_conn, db_curs, columnName, value, seriesID=wordSeriesID)	

class EMF_DataGenerator_Handle:
	def __init__(self, db_connection, db_cursor):
		self.db_conn = db_connection
		self.db_curs = db_cursor
		self.wordSeriesID = None
		self.dataSeriesID = None

	def __getFromDB(self, columnName):
		return EM_DBHelp.retrieve_DataSeriesMetaData(self.db_conn, self.db_curs, columnName, seriesID=self.dataSeriesID)

	def __sendToDB(self, columnName, value):
		return EM_DBHelp.update_WordSeriesMetaData(db_conn, db_curs, columnName, value, seriesID=wordSeriesID)

	def getDataHistory(self, dataType='float'):
		dataSeries = EM_DBHelp.getCompleteDataHistory_DataHistoryTable(self.db_conn, self.db_curs, self.dataSeriesID)
		dt = np.dtype('int,'+dataType) # Format is, e.g.  'int,int' or 'int,float'
		dataSeries = np.array(dataSeries, dtype=dt)
		dataSeries.dtype.names = ['dates','values']		
		return dataSeries

	def findAndStoreDataSeries(self, ticker):
		assert self.dataSeriesID is None # Force reset before setting (Or new object)

		self.dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(	self.db_conn, self.db_curs, 
																dataTicker=ticker, 
																insertIfNot=False)
		self.dataPeriodicity = self.__getFromDB('int_data_periodicity')

	def resetDataSeries(self):
		self.dataSeriesID = None
		self.dataPeriodicity = None

	def findAndStoreWordSeries(self, wordTicker, wordSubType, wordType, wordSuperType, insertIfNot=True):
		assert self.wordSeriesID is None  # Force reset before setting (Or new object)
		assert self.dataSeriesID is not None

		self.wordSeriesID = EM_DBHelp.retrieve_WordSeriesID(	self.db_conn, self.db_curs, 
																self.dataSeriesID, wordType, wordSubType, wordSuperType,
																wordTicker=wordTicker, 
																insertIfNot=True)
		self.__sendToDB('int_word_periodicity', self.dataPeriodicity)

	def resetWordSeries(self):
		self.wordSeriesID = None
		self.wordTicker = None

	def insertWords(self, dates, values):
		assert len(dates) == len(values)
		assert self.wordSeriesID is not None

		minDate = maxint
		maxDate = -maxint - 1

		successfulInserts = 0
		unsuccessfulInserts = 0

		for i in range(values):
			minDate = min(minDate, dates[i])
			maxDate = max(maxDate, dates[i])

			(success, error) = EM_DBHelp.insertDataPoint_WordHistoryTable(	self.db_conn, self.db_curs, 
																			self.wordSeriesID, 
																			dates[i], 
																			values[i])
			if not success:
				log.warning('\t\tFailed to Write Historical Word at %s for %s [value = %f]', wordTicker, date, value)
				unsuccessfulInserts += 1
			else:
				successfulInserts +=1
	
		log.info('\t%d Historical Words Written Successfuly for %s', successfulInserts, wordTicker)
		log.info('\t%d Historical Words Writing Failed for %s', unsuccessfulInserts, wordTicker)
		log.info('\tDate range from %s to %s Written for %s', EM_util.dtConvert_EpochtoY_M_D(minDate), EM_util.dtConvert_EpochtoY_M_D(maxDate), wordTicker)

		self.__sendToDB('int_unsuccessful_generations', unsuccessfulInserts)
		self.__sendToDB('dt_earliest_word', minDate)
		self.__sendToDB('dt_latest_word', maxDate)
		self.__sendToDB('dt_last_generated', EM_util.dtGetNowAsEpoch())
