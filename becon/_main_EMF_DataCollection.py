#TODO 1: Don't download replicate datapoints? (Or do, for revision purposes) (Especially if you can't specify that you want to retrieve less)
#TODO 2: Implement data score (is it autoregressive, what is regression with economic health key metrics, what is data length, does data extend to today)
#TODO 3: Change Series Loading/Storage Method from CSV to JSON
#TODO 4: Fill in ISM Data https://research.stlouisfed.org/fred2/search?st=napm
#TODO 5: Find a good Oil Price Source
import sqlite3 as sq
import logging as log
import datetime
import csv
import re
import copy #probably deprecated
from os.path import isfile
from calendar import monthrange
from sys import maxint
import fred # https://github.com/zachwill/fred
import fredapi # TODO: get latest releases from FREDAPI
import EMF_DatabaseHelper as EM_DBHelp
import EMF_DatabaseCreator as EM_DBMake
import EMF_util as EM_util

def downloadDataFromFred_fillMetadata(read_row, write_range=range(1,4)):
	write_row = copy.copy(read_row)
	write_range[write_range] = ['']*len(write_range)
	ticker = read_row[0]
	series_data = fred.series(ticker)
	if 'error_message' in series_data.keys():
		write_row[write_range[2]] = 'Metadata Error: ' + series_data['error_message']
	else:
		# Read Data
		data = series_data['seriess'][0]
		# Write Data to Database
		try:
			downloadDataFromFred_fillMetadata_parseSeriesData(ticker, data)
		except:
			raise
		finally:
			finalize()
		# Update Data in CSV
		write_row[write_range[2]] = 'Metadata Updated'
	write_row[write_range[0]] = datetime.datetime.now()
	write_row[write_range[1]] = datetime.datetime.now().strftime('%A, %d. %B %Y %I:%M%p')
	return write_row

def downloadDataFromFred_fillHistory(write_row, write_range=range(4,7)):
	for i in write_range: write_row[i] = ''
	ticker = write_row[0]
	log.info('\tFinding Historical Data for %s', ticker)
	historical_data = fred.observations(ticker)
	if 'error_message' in historical_data.keys():
		log.warning('\tError in Finding Historical Data for %s', ticker)
		log.warning('\tError: %s', historical_data['error_message'])
		write_row[write_range[2]] = 'Metadata Error: ' + historical_data['error_message']
	else:
		log.info('\tFound %d Historical Data Points for %s... Writing to DB', len(historical_data['observations']), ticker)
		# Write Data to Database
		try:
			# TODO: Check if we should even fill (i.e. if latest date is more than today's date)
			downloadDataFromFred_fillHistory_parseSeriesData(ticker, historical_data)
		except:
			raise
		# Update Data in CSV
		write_row[write_range[2]] = 'Metadata Updated'
	write_row[write_range[0]] = datetime.datetime.now()
	write_row[write_range[1]] = datetime.datetime.now().strftime('%A, %d. %B %Y %I:%M%p')
	return write_row

def downloadDataFromFred_fillHistory_parseSeriesData(dataSeriesTicker, data):
	dataSeriesID =	EM_DBHelp.retrieve_DataSeriesID(db_connection, db_cursor, 
													dataTicker=dataSeriesTicker, 
													insertIfNot=True)
	sendToDB = lambda: \
	EM_DBHelp.update_DataSeriesMetaData(db_connection, db_cursor, columnName, value, seriesID=dataSeriesID)

	successfulInserts = 0
	unsuccessfulInserts = 0
	minObsDate = maxint
	maxObsDate = -maxint-1
	# Update History
	for obs in data['observations']:
		if obs['value'] == '.':
			continue # Avoid Unfilled FRED Values
		date = EM_util.dtConvert_YYYY_MM_DDtoEpoch(obs['date'])
		value = float(obs['value'])
		maxObsDate = date if date > maxObsDate else maxObsDate
		minObsDate = date if date < minObsDate else minObsDate
		log.info('\t\tWriting Historical Data Point at %s for %s [value = %f]', dataSeriesTicker, obs['date'], value)
		(success, error) = EM_DBHelp.insertDataPoint_DataHistoryTable(	
													db_connection, db_cursor,  
													dataSeriesID, 
													date, 
													value, 
													isInterpolated=False)
		if not success:
			log.warning('\t\tFailed to Write Historical Data Point at %s for %s [value = %f]', dataSeriesTicker, obs['date'], obs['value'])
			unsuccessfulInserts += 1
		else:
			successfulInserts +=1
	
	log.info('\t%d Historical Data Points Written Successfuly for %s', successfulInserts, dataSeriesTicker)
	log.info('\t%d Historical Data Points Written Failed for %s', unsuccessfulInserts, dataSeriesTicker)
	log.info('\tDate range from %s to %s Written for %s', EM_util.dtConvert_EpochtoY_M_D(minObsDate), EM_util.dtConvert_EpochtoY_M_D(maxObsDate), dataSeriesTicker)

	# Update Successful Inserts Flag
	columnName = 'int_unsuccessful_inserts'
	value = unsuccessfulInserts
	sendToDB()

	# Update Earliest DataPoint Found
	columnName = 'dt_earliest_value'
	value = minObsDate
	sendToDB()

	# Update Latest DataPoint Found
	columnName = 'dt_latest_value'
	value = maxObsDate
	sendToDB()

	# Update Last Update Date
	columnName = 'dt_last_updated_history'
	value = EM_util.dtGetNowAsEpoch()
	sendToDB()

#DATA FROM FRED	
# (int_ID INTEGER UNIQUE NOT NULL PRIMARY KEY,
	# realtime_start,
	# realtime_end Char(250),
	# title Char(250),
	# observation_start Char(250),
	# observation_end Char(250),
	# frequency Char(250),
	# frequency_short Char(250),
# units Char(250),
# units_short Char(250),
	# seasonal_adjustment Char(250),
	# seasonal_adjustment_short Char(250),
	# last_updated Char(250),
# popularity Char(250),
# notes Char(250)

#DATA IN T_ECONOMIC_DATA DB
	# int_data_master_id INTEGER UNIQUE NOT NULL PRIMARY KEY,
	# txt_data_name TEXT NOT NULL,
	# txt_data_ticker TEXT NOT NULL UNIQUE,
	# int_data_periodicity INTEGER, /* periodicity in 1/year. Days is 365; weeks is 52; months is 12; quarters is 4 */
# dt_earliest_value INTEGER, /* earliest data point we have */
# dt_latest_value INTEGER, /* latest data point we have */
# dt_last_updated INTEGER, /* when we got the data last */
# int_unsuccessful_inserts INTEGER DEFAULT 0


	# int_data_master_id INTEGER NOT NULL PRIMARY KEY,
# txt_data_source TEXT,
	# dt_last_updated_SOURCE INTEGER, /* last update according to FRED */
	# dt_earliest_value_SO INTEGER, /* earliest date possible according to FRED */
	# dt_latest_value_SO INTEGER, /* latest date possible according to FRED */
	# bool_generated_datapoint INTEGER,
# bool_is_normalized INTEGER,
	# bool_is_seasonally_adjusted INTEGER,
# bool_is_real INTEGER,
# bool_is_deflator INTEGER,
# code_private_public INTEGER,
# code_economic_activity INTEGER,
# code_data_adjustment INTEGER,
# code_sector INTEGER,
# code_data_units INTEGER,
# code_item_type INTEGER

per_matchObj = r'(.*) per (.*)'
of_matchObj = r'(.*) of (.*)'
IndexEquals_matchObj = r'Index (.*)=(.*)'
year_matchObj = r'(\d\d\d\d)(?:-\d\d)?'

def downloadDataFromFred_fillMetadata_parseSeriesData(dataSeriesTicker, data):
	# Get Data Series Name
	dataSeriesName = data['title'].encode('ascii', 'ignore')
	# Save Off Data Series Real/Nominal Flag for Later Use
	dataSeriesIsRealValue = None
	# Create or Get Series ID
	dataSeriesID =	EM_DBHelp.retrieve_DataSeriesID(db_connection, db_cursor, 
													dataName=dataSeriesName, 
													dataTicker=dataSeriesTicker, 
													insertIfNot=True)

	sendToDB = lambda: \
	EM_DBHelp.update_DataSeriesMetaData(db_connection, db_cursor, columnName, value, seriesID=dataSeriesID)

	# Insert Data Series Periodicity
	dataFrequency = data['frequency_short']
	if dataFrequency == 'M':
		value = 12
	elif dataFrequency == 'Q':
		value = 4
	elif dataFrequency == 'A':
		value = 1
	elif dataFrequency == 'D':
		raise NotImplementedError('Not even sure if this is a category')	
	else:
		raise NotImplementedError('Data Frequency not recognized')
	columnName = 'int_data_periodicity'
	sendToDB()

	# Get Data Series Seasonal Adjustment
	dataSeasonalAdjustment = data['seasonal_adjustment_short']
	if dataSeasonalAdjustment == 'SA' or dataSeasonalAdjustment == 'SAAR':
		value = 1
	elif dataSeasonalAdjustment == 'NSA':
		value = 0
	else:
		raise NotImplementedError('Data Seasonal Adjustment Code not recognized')
	columnName = 'bool_is_seasonally_adjusted'
	sendToDB()

	# Get Data Series Last Updated
	value = EM_util.dtConvert_YYYY_MM_DD_TimetoEpoch(data['last_updated'])
	columnName = 'dt_last_updated_SOURCE'
	sendToDB()

	# Get Data Series First Value
	value = EM_util.dtConvert_YYYY_MM_DDtoEpoch(data['observation_start'])
	columnName = 'dt_earliest_value_SOURCE'
	sendToDB()

	# Get Data Series Last Value
	value = EM_util.dtConvert_YYYY_MM_DDtoEpoch(data['observation_end'])
	columnName = 'dt_latest_value_SOURCE'
	sendToDB()
	
	# Fill Generated Flag (Always False)
	value = 0
	columnName = 'bool_generated_datapoint'
	sendToDB()

	# Get Information From Units
	dataUnits = data['units']
	done = 0
	if dataUnits == 'Percent':
		dataUnitsValue = 'Percent'
		dataTypeValueValue = 'Percent'
		dataSeriesIsRealValue = False #Consider, true if something like unemployment rate, false if bond yield
		done = 1
	elif dataUnits == 'Number':
		dataUnitsValue = 'Number'
		dataTypeValue = 'Number'
		dataSeriesIsRealValue = False #Consider, true?
		done = 1
	elif dataUnits == 'Index':
		dataUnitsValue = 'Number'
		dataTypeValue = 'Index'
		dataSeriesIsRealValue = False
		done = 1
	matchObj = None if done else re.match(of_matchObj, dataUnits)
	if (matchObj is not None):
		dataUnitsValue = matchObj.group(1)
		dataTypeValue = matchObj.group(2)
		innerMatchObj = re.match(ChainedDollars_matchObj,dataTypeValue)
		if (innerMatchObj is not None):
			dataSeriesIsRealValue = True
			dataTypeValue = innerMatchObj.group(2)
		else:
			dataSeriesIsRealValue = False
		done = 1
	matchObj = None if done else re.match(per_matchObj, dataUnits)
	if (matchObj is not None):
		dataUnitsValue = 'per ' + matchObj.group(2)
		dataTypeValue = matchObj.group(1)
		done = 1
	matchObj = None if done else re.match(IndexEquals_matchObj, dataUnits)
	if (matchObj is not None):
		dataUnitsValue = 'per ' + matchObj.group(2)
		dataTypeValue = matchObj.group(1)
		done = 1

	if dataSeriesIsRealValue is None:
		raise NotImplementedError
		# dataSeriesIsRealValue = 
	columnName = 'bool_is_real' #TODO: regex
	columnName = 'bool_is_normalized' #TODO: regex

	#TODO: Deal with Units


def downloadDataFromFred(csvFileName=EM_util.FREDSeriesCSV, fillHistory=True, fillMetadata=True):
	# Access Global Variables
	global verboseLevel
	global db_cursor
	global db_connection

	# Connect to FRED
	fred.key(EM_util.FREDAPIKey)

	# Read CSV file
	log.info('Accessing CSV to get Series Tickers...')
	with open(csvFileName, 'rU') as csvfile:
		series_csv = csv.reader(csvfile)
		header = [next(series_csv)] # Ignore header in CSV
		write_list = [list(l) for l in series_csv]
		log.info('Downloading Series data to sqlite...')
		for i in range(len(write_list)):	
			if fillHistory:
				write_list[i] = downloadDataFromFred_fillHistory(write_list[i])
			if fillMetadata:
				write_list[i] = downloadDataFromFred_fillMetadata(write_list[i])
		log.info('Downloaded Series data sucessfully...')

	with open(csvFileName, 'wb') as csvfile:
		log.info('Updating Series CSV File...')
		csvwrite=csv.writer(csvfile)
		csvwrite.writerows(header)
		csvwrite.writerow(write_list)
		log.info('CSV file updated...')

def performInitialSetup(DBFilePath=None, forceDBCreation=False):
	# Establish Global Variables
	global db_cursor
	global db_connection

	if DBFilePath is None:
		DBFilePath = EM_util.defaultDB
	
	log.info('Connecting to Database: \n%s', DBFilePath)
	
	# Create Database
	if not isfile(DBFilePath):
		log.info('Database not found. Creating new database...')
		EM_DBMake.doOneTimeDBCreation(force=forceDBCreation, DBFilePath=DBFilePath)

	# Create Database Connection
	db_connection = sq.connect(DBFilePath)
	db_cursor = db_connection.cursor()
	log.info('Database opened successfully')

def finalize():
	db_connection.close()

if __name__=="__main__":
	try:
		performInitialSetup()
		downloadDataFromFred() #metadata not ready yet
	except:
		raise
	finally:
		finalize()
