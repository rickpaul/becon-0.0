#TODO 1: Don't download replicate datapoints? (Or do, for revision purposes) (Especially if you can't specify that you want to retrieve less)
#TODO 2: Implement data score (is it autoregressive, what is regression with economic health key metrics, what is data length, does data extend to today)
#TODO 3: Change Series Loading/Storage Method from CSV to JSON
#TODO 4: Fill in ISM Data https://research.stlouisfed.org/fred2/search?st=napm
#TODO 5: Find a good Oil Price Source
#TODO 6: Close/update csv even if error occurred
import sqlite3 as sq
import logging as log
import datetime
import csv
import re
import copy #probably deprecated
from os.path import isfile
from os import rename
from calendar import monthrange
from sys import maxint
import fred # https://github.com/zachwill/fred
import fredapi # TODO: get latest releases from FREDAPI
import EMF_DatabaseHelper as EM_DBHelp
import EMF_DatabaseCreator as EM_DBMake
import EMF_util as EM_util

FREDCol = EM_util.FREDSeriesCSVCols

def downloadDataFromFred_fillMetadata(ticker):
	series_data = fred.series(ticker)
	log.info('\tFinding Metadata for %s', ticker)
	if 'error_message' in series_data.keys():
		log.warning('\tError in Finding Metadata for %s', ticker)
		log.warning('\tError: %s', historical_data['error_message'])		
		status = 'ERROR: ' + series_data['error_message']
	else:
		log.info('\tFound Metadata for %s... Writing to DB', ticker)
		# Read Data
		metadata = series_data['seriess'][0]
		# Write Data to Database
		try:
			downloadDataFromFred_fillMetadata_parseSeriesData(ticker, metadata)
		except:
			raise
		# Update Data in CSV
		status = 'ok'
	return status

def downloadDataFromFred_fillHistory(ticker):
	log.info('\tFinding Historical Data for %s', ticker)
	historical_data = fred.observations(ticker)
	if 'error_message' in historical_data.keys():
		log.warning('\tError in Finding Historical Data for %s', ticker)
		log.warning('\tError: %s', historical_data['error_message'])
		status = 'ERROR: ' + historical_data['error_message']
	else:
		log.info('\tFound %d Historical Data Points for %s... Writing to DB', len(historical_data['observations']), ticker)
		# Write Data to Database
		try:
			# TODO: Check if we should even fill (i.e. if latest date is more than today's date)
			downloadDataFromFred_fillHistory_parseSeriesData(ticker, historical_data)
		except:
			raise
		# Update Data in CSV
		status = 'ok'
	return status

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

per_matchObj = r'(.*) per (.*)'
of_matchObj = r'(.*) of (.*)'
IndexEquals_matchObj = r'Index (.*)=(.*)'
ChainedDollars_matchObj = r'Chained \d\d\d\d Dollars'
year_matchObj = r'(\d\d\d\d)(?:-\d\d)?'

def downloadDataFromFred_fillMetadata_parseSeriesData(dataSeriesTicker, data):
	#Get Data Series ID
	dataSeriesID =	EM_DBHelp.retrieve_DataSeriesID(db_connection, db_cursor, 
													dataTicker=dataSeriesTicker, 
													insertIfNot=True)
	sendToDB = lambda: \
	EM_DBHelp.update_DataSeriesMetaData(db_connection, db_cursor, columnName, value, seriesID=dataSeriesID)

	# Update Data Series Name
	columnName = 'txt_data_name'
	value = data['title'].encode('ascii', 'ignore')
	sendToDB()

	columnName = 'txt_data_source'
	value = 'FRED'
	sendToDB()

	# Update Data Series Periodicity
	dataFrequency = data['frequency_short']
	if dataFrequency == 'M':
		value = 12
	elif dataFrequency == 'Q':
		value = 4
	elif dataFrequency == 'A':
		value = 1
	elif dataFrequency == 'W':
		value = 52		
	elif dataFrequency == 'D':
		value = 365
	else:
		raise NotImplementedError('Data Frequency not recognized')
	columnName = 'int_data_periodicity'
	sendToDB()

	# Update Data Series Seasonal Adjustment
	dataSeasonalAdjustment = data['seasonal_adjustment_short']
	if dataSeasonalAdjustment == 'SA' or dataSeasonalAdjustment == 'SAAR':
		value = 1
	elif dataSeasonalAdjustment == 'NSA':
		value = 0
	else:
		raise NotImplementedError('Data Seasonal Adjustment Code not recognized')
	columnName = 'bool_is_seasonally_adjusted'
	sendToDB()

	# Update Data Series Last Updated
	value = EM_util.dtConvert_YYYY_MM_DD_TimetoEpoch(data['last_updated'])
	columnName = 'dt_last_updated_SOURCE'
	sendToDB()

	# Update Data Series First Value
	value = EM_util.dtConvert_YYYY_MM_DDtoEpoch(data['observation_start'])
	columnName = 'dt_earliest_value_SOURCE'
	sendToDB()

	# Update Data Series Last Value
	value = EM_util.dtConvert_YYYY_MM_DDtoEpoch(data['observation_end'])
	columnName = 'dt_latest_value_SOURCE'
	sendToDB()
	
	# Fill Generated Flag (Always False)
	value = 0
	columnName = 'bool_generated_datapoint'
	sendToDB()

	# Update Last Update Date
	columnName = 'dt_last_updated_metadata'
	value = EM_util.dtGetNowAsEpoch()
	sendToDB()

	# Update Information From Units	
	# TEST! TEST! TEST! TEST! TEST! TEST! TEST! TEST!
	# TEST! TEST! TEST! TEST! TEST! TEST! TEST! TEST!
	# BELOW HERE WE CHANGE TO NOT ACTUALLY UPDATE DB.
	# TEST! TEST! TEST! TEST! TEST! TEST! TEST! TEST!
	# TEST! TEST! TEST! TEST! TEST! TEST! TEST! TEST!

	# Save Off Data Series Real/Nominal Flag for Later Use
	dataSeriesIsRealValue = None	
	def sendToDB(): print data['title'] + '|' + data['units'] + '|' + columnName + ' : ' + str(value) #TEST
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
			dataTypeValue = 'Dollars'
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
		dataUnitsValue = 'Number'
		dataTypeValue = 'Index'
		done = 1

	if dataSeriesIsRealValue is None:
		pass
		# raise NotImplementedError 
		# dataSeriesIsRealValue = #TODO: regex: Read data series name to determine (if it has the word 'real')
	columnName = 'bool_is_real' 
	value = dataSeriesIsRealValue
	sendToDB()
	columnName = 'bool_is_normalized' #TODO: fill
	value = None
	sendToDB()
	columnName = 'code_private_public' #TODO: fill
	value = None
	sendToDB()	
	columnName = 'code_economic_activity' #TODO: fill
	value = None
	sendToDB()
	columnName = 'code_data_adjustment' #TODO: fill
	value = None
	sendToDB()
	columnName = 'code_sector' #TODO: fill
	value = None
	sendToDB()	
	columnName = 'code_data_units' #TODO: fill
	value = None
	sendToDB()	
	columnName = 'code_item_type' #TODO: fill
	value = None
	sendToDB()
	columnName = 'txt_data_original_source' #TODO: fill
	value = None
	sendToDB()


def downloadDataFromFred(	csvFileName=EM_util.FREDSeriesCSV, 
							fillHistory=True, 
							fillMetadata=True, 
							fillUserData=True, 
							minImportance=2, 
							writeEveryRow=True,
							pause=False):
	# Access Global Variables
	global db_cursor
	global db_connection

	# Establish Helpful Lambdas
	sendToDB = lambda: \
	EM_DBHelp.update_DataSeriesMetaData(db_connection, db_cursor, columnName, value, seriesID=dataSeriesID)

	# Read CSV file
	log.info('Accessing CSV to get Series Tickers...')
	with open(csvFileName, 'rU') as csvfile:
		series_csv = csv.reader(csvfile)
		header = [next(series_csv)] # Ignore header in CSV
		write_list = [list(row) for row in series_csv]
		log.info('Downloading FRED data to database...')
		for i in range(len(write_list)):
			# Recognize End of File without Reaching Deprecated
			if write_list[i][FREDCol['TICKER_COL']] == '':
				break
			if int(write_list[i][FREDCol['IMPORTANCE_COL']]) > minImportance:
				continue
			lastHistoryDownload = 	datetime.datetime.strptime(
									write_list[i][FREDCol['HISTORY_TIMESTAMP_COL']],
									EM_util.FREDSeriesCSVTimeFormat)
			dnldHistory = 	((fillHistory and \
							(datetime.datetime.now()-lastHistoryDownload) > EM_util.FREDDownloadDelayHistory) or \
							write_list[i][FREDCol['FORCE_HISTORY_REDOWNLOAD_COL']] == EM_util.FREDForceDownload)  and \
							write_list[i][FREDCol['FORCE_HISTORY_REDOWNLOAD_COL']] != EM_util.FREDSkipDownload

			lastMetadataDownload = 	datetime.datetime.strptime(
									write_list[i][FREDCol['HISTORY_TIMESTAMP_COL']],
									EM_util.FREDSeriesCSVTimeFormat)
			dnldMetadata = 	((fillMetadata and\
							(datetime.datetime.now()-lastMetadataDownload) > EM_util.FREDDownloadDelayMetadata) or \
							write_list[i][FREDCol['FORCE_METADATA_REDOWNLOAD_COL']] == EM_util.FREDForceDownload)  and \
							write_list[i][FREDCol['FORCE_METADATA_REDOWNLOAD_COL']] != EM_util.FREDSkipDownload

			ticker = write_list[i][FREDCol['TICKER_COL']]
			log.info('Downloading %s data to database...', ticker)
			if dnldHistory:
				status = downloadDataFromFred_fillHistory(ticker)
				write_list[i][FREDCol['HISTORY_STATUS_COL']] = status
				write_list[i][FREDCol['HISTORY_TIMESTAMP_COL']] = \
					datetime.datetime.now().strftime(EM_util.FREDSeriesCSVTimeFormat)
			if dnldMetadata:
				status = downloadDataFromFred_fillMetadata(ticker)
				write_list[i][FREDCol['METADATA_STATUS_COL']] = status
				write_list[i][FREDCol['METADATA_TIMESTAMP_COL']] = \
					datetime.datetime.now().strftime(EM_util.FREDSeriesCSVTimeFormat)
			# TODO: Decide when to fillUserData
			if fillUserData:
				# We seek the Series ID again to make sure it was input correctly earlier
				# (Rather than just passing it back from earlier)	
				dataSeriesID = EM_DBHelp.retrieve_DataSeriesID(db_connection, db_cursor, 
																dataTicker=ticker, 
																insertIfNot=False)
				columnName = 'code_data_subtype'
				csvVal = write_list[i][FREDCol['SUBTYPE_COL']]
				if csvVal == '': csvVal = 'unknown'			
				value = int(EM_util.dataSubtypes[csvVal])
				sendToDB()
				
				columnName = 'code_data_type'
				csvVal = write_list[i][FREDCol['TYPE_COL']]
				if csvVal == '': csvVal = 'unknown'
				value = int(EM_util.dataTypes[csvVal])
				sendToDB()
				
				columnName = 'code_data_supertype'
				csvVal = write_list[i][FREDCol['SUPERTYPE_COL']]
				if csvVal == '': csvVal = 'unknown'
				value = int(EM_util.dataSupertypes[csvVal])
				sendToDB()

				columnName = 'code_is_level'
				csvVal = write_list[i][FREDCol['IS_LEVEL_COL']]
				if csvVal == '': csvVal = 'unknown'
				value = int(EM_util.levelMattersTypes[csvVal])
				sendToDB()

				columnName = 'code_good_direction'
				csvVal = write_list[i][FREDCol['POSITIVE_IS_GOOD_COL']]
				if csvVal == '': csvVal = 'unknown'
				value = int(EM_util.goodDirectionTypes[csvVal])
				sendToDB()
			# TODO: This is hacky. Do better.
			if writeEveryRow:
				with open(csvFileName[:-4]+'_temp.csv', 'wb') as milestone_csvfile:
					log.info('Updating Series CSV File...')
					csvwrite = csv.writer(milestone_csvfile)
					csvwrite.writerows(header)
					csvwrite.writerows(write_list)
					log.info('CSV file updated...')

			# For Testing... TODO: Remove
			if pause: 
				statement = raw_input('\nPress Enter to Continue...\n') 

		log.info('Downloaded Series data sucessfully...')

	with open(csvFileName, 'wb') as csvfile:
		log.info('Updating Series CSV File...')
		csvwrite = csv.writer(csvfile)
		csvwrite.writerows(header)
		csvwrite.writerows(write_list)
		log.info('CSV file updated...')

def performInitialSetup(DBFilePath=None, forceDBCreation=False, logFilePath=None, recordLog=False, quietShell=False):
	# Establish Global Variables
	global db_cursor
	global db_connection

	# Initialize Log
	if quietShell and not recordLog:
		recordLevel=log.INFO
	else:
		recordLevel=None

	EM_util.initializeLog(recordLog=recordLog, logFilePath=logFilePath, recordLevel=recordLevel)
	log.info('Log Initialized.')

	# Connect to FRED
	log.info('Connecting to FRED.')
	fred.key(EM_util.FREDAPIKey)
	
	# Create Database
	log.info('Connecting to Database: \n%s', DBFilePath)
	if DBFilePath is None:
		DBFilePath = EM_util.defaultDB
		
	if not isfile(DBFilePath):
		log.info('Database not found. Creating new database...')
		EM_DBMake.doOneTimeDBCreation(force=forceDBCreation, DBFilePath=DBFilePath)

	# Store Database Connection
	db_connection = sq.connect(DBFilePath)
	db_cursor = db_connection.cursor()
	log.info('Database opened successfully')

def finalize():
	db_connection.close()

if __name__=="__main__":
	from sys import argv
	try:
		args = argv[1:]
		recordLog = 1 if '-r' in args else 0
		quietShell = 1 if '-q' in args else 0
		performInitialSetup(recordLog=recordLog,quietShell=quietShell)
		downloadDataFromFred()
	except:
		raise
	finally:
		finalize()
