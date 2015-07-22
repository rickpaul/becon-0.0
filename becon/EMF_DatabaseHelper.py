import sqlite3 as sq
import logging as log
from string import join, find, lower
from pprint import pprint
from numpy import int64, float64
from EMF_DatabaseCreator import DataNameDataTableLink
from EMF_util import defaultDB

SQL_NULL = 'NULL' # Sqlite null

########################################Generic Query Construction Code / Helper Code
def stringify(someValue):
	if type(someValue) == bool:
		return str(int(someValue))
	elif type(someValue) == int or type(someValue) == float:
		return str(someValue)
	elif type(someValue) == int64 or type(someValue) == float64:
		return str(someValue)
	elif someValue is None:
		return SQL_NULL
	elif (someValue == SQL_NULL) or (someValue[0] is '"') or (someValue[0] is "'"):
		return someValue
	else:
		return '"' + someValue + '"'

########################################Generic Query Construction Code / Insert Code
def generateInsertStatement(table, columns, values):
	columnsString = ' ( ' + join(columns,', ') + ' ) '
	valuesString = ' ( ' + join([stringify(v) for v in values],', ') + ' ) '
	return ('insert into ' + table + columnsString + 'values' + valuesString + ';')

########################################Generic Query Construction Code / Update Code
def generateUpdateStatement(table, setColumns, setValues, whereColumns, whereValues):
	setStatements = [(a + '=' + stringify(b)) for (a,b) in zip(setColumns,setValues)]
	whereStatements = [(a + '=' + stringify(b)) for (a,b) in zip(whereColumns,whereValues)]
	setString = join(setStatements,' , ')
	whereString = join(whereStatements,' and ')
	return ('update ' + table + ' set ' + setString + ' where ' + whereString + ';')

########################################Generic Query Construction Code / Select Code
def generateSelectStatement(table, 
							selectColumns=None, 
							selectCount=False, 
							whereColumns=None, whereValues=None, 
							lessThanColumns=None, lessThanValues=None,
							moreThanColumns=None, moreThanValues=None,
							orderBy=None, limit=None):
	if selectCount:
		columnsString = ' count(*) '
	elif selectColumns is None:
		columnsString = ' * '
	else:
		columnsString = ' ' + join(selectColumns,', ') + ' '
	if whereColumns is not None or lessThanColumns is not None or moreThanColumns is not None:
		whereStatements = []
		if whereColumns is not None:
			for (a,b) in zip(whereColumns,whereValues):
				if type(b) is not list:
					whereStatements.append(a + '=' + stringify(b))
				else:
					tempList = [(a + '=' + stringify(c)) for c in b]
					temp = '('
					temp += join(tempList,' or ')
					temp += ')'
					whereStatements.append(temp)
		if lessThanColumns is not None:
			for (a,b) in zip(lessThanColumns,lessThanValues):
				whereStatements.append(a + '<=' + stringify(b))
		if moreThanColumns is not None:
			for (a,b) in zip(moreThanColumns,moreThanValues):
				whereStatements.append(a + '>=' + stringify(b))		
		whereString = ' where ' + join(whereStatements,' and ')
	else:
		whereString = ''
	if orderBy is not None:
		orderByString = ' order by ' + join(orderBy[0],', ') + ' ' + orderBy[1] + ' '
	else:
		orderByString = ''		
	if limit is not None:
		limitString = ' limit ' + str(limit)
	else:
		limitString = ''

	return ('select ' + columnsString + ' from ' + table + whereString + orderByString + limitString + ';')


########################################Specific Query Construction Code / Update Metadata by column
def __get_update_DataSeriesMetaData_Statement(seriesID, columnName, value):
	table = DataNameDataTableLink[columnName]
	setColumns = [columnName]
	setValues = [value]
	whereColumns = ['int_data_master_id']
	whereValues = [seriesID]
	return generateUpdateStatement(table, setColumns, setValues, whereColumns, whereValues)

# TODO: Remove ability to access value by dataname, dataticker
def update_DataSeriesMetaData(	conn, cursor, 
								columnName, value, 
								seriesID=None, dataName=None, dataTicker=None):
	if seriesID is None:
		seriesID = retrieve_DataSeriesID(conn, cursor, dataName=dataName, dataTicker=dataTicker)
		if seriesID is None:
			raise Exception('Data Series {0} [{1}] not found in Database'.format(dataTicker, dataName))
	statement = __get_update_DataSeriesMetaData_Statement(seriesID, columnName, value)
	return commitDBStatement(conn, cursor, statement)[0]

########################################Specific Query Construction Code / Retrieve Metadata by column
def __get_retrieve_DataSeriesMetaData_Statement(seriesID, columnName):
	table = DataNameDataTableLink[columnName]
	selectColumns = [columnName]
	whereColumns = ['int_data_master_id']
	whereValues = [seriesID]
	return generateSelectStatement(	table, 
									whereColumns=whereColumns, 
									whereValues=whereValues, 
									selectColumns=selectColumns)

# TODO: Remove ability to access value by dataname, dataticker
def retrieve_DataSeriesMetaData(conn, cursor, 
								columnName, 
								seriesID=None, dataName=None, dataTicker=None):
	if seriesID is None:
		seriesID = retrieve_DataSeriesID(conn, cursor, dataName=dataName, dataTicker=dataTicker)
		if seriesID is None:
			raise Exception('Data Series {0} [{1}] not found in Database'.format(dataTicker, dataName))
	statement = __get_retrieve_DataSeriesMetaData_Statement(seriesID, columnName)
	cursor.execute(statement)
	results = cursor.fetchall()
	if len(results) != 1:
		log.error(statement)
		raise Exception('Improper results for query')
	return results[0][0]

########################################Specific Query Construction Code / Find Data Series ID 
def __getWhereColumnValuesForNameAndTicker(dataName=None, dataTicker=None):
	if dataTicker is not None:
		whereColumns = ['txt_data_ticker']
		whereValues = [dataTicker]
	elif dataName is not None: # We prioritise dataTicker over dataName
		whereColumns = ['txt_data_name']
		whereValues = [dataName]
	else:
		raise Exception('No Ticker/Name given for update')
	return (whereColumns, whereValues)

def __get_retrieve_DataSeriesID_Statement(dataName=None, dataTicker=None):
	table = 'T_ECONOMIC_DATA'
	(whereColumns, whereValues) = __getWhereColumnValuesForNameAndTicker(dataName=dataName, dataTicker=dataTicker)
	return generateSelectStatement(	table, 
									whereColumns=whereColumns, 
									whereValues=whereValues, 
									selectColumns=['int_data_master_id'])

def retrieve_DataSeriesID(conn, cursor, dataName=None, dataTicker=None, insertIfNot=False):
	statement = __get_retrieve_DataSeriesID_Statement(dataName=dataName, dataTicker=dataTicker)
	cursor.execute(statement)
	seriesID = cursor.fetchone()
	if seriesID is None and insertIfNot:
		log.info('Series ID Not Found for %s... Creating.', dataTicker)
		# Add SeriesID, Name, Ticker to T_ECONOMIC_DATA
		table = 'T_ECONOMIC_DATA'
		columns = ['txt_data_name', 'txt_data_ticker']
		values = [dataName, dataTicker]
		statement = generateInsertStatement(table, columns, values)			
		cursor.execute(statement)
		seriesID = cursor.lastrowid
		log.info('Series ID Created for %s (%d)', dataTicker, seriesID)
		# Add SeriesID to T_ECONOMIC_DATA_DETAILED
		table = 'T_ECONOMIC_DATA_DETAILED'
		columns = ['int_data_master_id']
		values = [seriesID]
		statement = generateInsertStatement(table, columns, values)			
		cursor.execute(statement)
		# Add SeriesID to T_ECONOMIC_DATA_EXTRANEOUS
		table = 'T_ECONOMIC_DATA_EXTRANEOUS'
		columns = ['int_data_master_id']
		values = [seriesID]
		statement = generateInsertStatement(table, columns, values)			
		cursor.execute(statement)
		# Commit and Return
		conn.commit()
		return seriesID
	else:
		log.info('Series ID Found for %s (%d).', dataTicker, seriesID[0])
		return int(seriesID[0])

########################################Specific Query Construction Code / Insert New Historical Data Point
def __get_insertDataPoint_DataHistoryTable_Statement(seriesID, dataPointDate, dataValue, isInterpolated=None):
	table = 'T_ECONOMIC_DATA_HISTORY'
	columns = ['int_data_master_id', 'dt_data_time', 'flt_data_value']
	values = [seriesID, dataPointDate, dataValue]
	if isInterpolated is not None:
		columns.append('bool_is_interpolated')
		values.append(isInterpolated)
	return generateInsertStatement(table, columns, values)

def insertDataPoint_DataHistoryTable(conn, cursor, seriesID, dataPointDate, dataValue, isInterpolated=None):
	statement = __get_insertDataPoint_DataHistoryTable_Statement(seriesID, dataPointDate, dataValue,isInterpolated=isInterpolated)
	return commitDBStatement(conn, cursor, statement)


########################################Specific Query Construction Code / Insert New Historical Data Point
def __get_completeDataHistory_DataHistoryTable_Statement(seriesID):
	table = 'T_ECONOMIC_DATA_HISTORY'
	whereColumns = ['int_data_master_id']
	whereValues = [seriesID]
	selectColumns = ['dt_data_time','flt_data_value']
	statement = generateSelectStatement(table, 
										whereColumns=whereColumns, 
										whereValues=whereValues, 
										selectColumns=selectColumns,
										orderBy=(['dt_data_time'], 'ASC'))
	return statement

def getCompleteDataHistory_DataHistoryTable(conn, cursor, seriesID):
	statement = __get_completeDataHistory_DataHistoryTable_Statement(seriesID)
	cursor.execute(statement)
	series = cursor.fetchall()
	return series

########################################Specific Query Construction Code / Update Metadata by column
# TODO: More or less a copy of DataSeriesMetaData... Combine
def __get_update_WordSeriesMetaData_Statement(seriesID, columnName, value):
	table = DataNameDataTableLink[columnName]
	setColumns = [columnName]
	setValues = [value]
	whereColumns = ['int_word_master_id']
	whereValues = [seriesID]
	return generateUpdateStatement(table, setColumns, setValues, whereColumns, whereValues)

# TODO: More or less a copy of DataSeriesMetaData... Combine
def update_WordSeriesMetaData(	conn, cursor, columnName, value, seriesID):
	statement = __get_update_WordSeriesMetaData_Statement(seriesID, columnName, value)
	return commitDBStatement(conn, cursor, statement)[0]

########################################Specific Query Construction Code / Retrieve Metadata by column
# TODO: More or less a copy of DataSeriesMetaData... Combine
def __get_retrieve_WordSeriesMetaData_Statement(seriesID, columnName):
	table = DataNameDataTableLink[columnName]
	selectColumns = [columnName]
	whereColumns = ['int_word_master_id']
	whereValues = [seriesID]
	statement = generateSelectStatement(table, 
										whereColumns=whereColumns, 
										whereValues=whereValues, 
										selectColumns=selectColumns)
	return statement

# TODO: More or less a copy of DataSeriesMetaData... Combine
def retrieve_WordSeriesMetaData(conn, cursor, columnName, seriesID):
	statement = __get_retrieve_WordSeriesMetaData_Statement(seriesID, columnName)
	cursor.execute(statement)
	results = cursor.fetchall()
	if len(results) != 1:
		log.error(statement)
		raise Exception('Improper results for query')
	return results[0][0]

########################################Specific Query Construction Code / Insert New Word for Historical Data Words
def __get_insertDataPoint_WordHistoryTable_Statement(wordSeriesID, dataPointDate, dataValue):
	table = 'T_ECONOMIC_DATA_WORDS_HISTORY'
	columns = ['int_word_master_id', 'dt_data_time', 'flt_data_value']
	values = [wordSeriesID, dataPointDate, dataValue]
	return generateInsertStatement(table, columns, values)

def insertDataPoint_WordHistoryTable(conn, cursor, wordSeriesID, dataPointDate, dataValue):
	statement = __get_insertDataPoint_WordHistoryTable_Statement(wordSeriesID, dataPointDate, dataValue)
	return commitDBStatement(conn, cursor, statement)


########################################Specific Query Construction Code / Insert New Historical Data Words
def __get_wordHistory_WordHistoryTable_Statement(wordSeriesID, beforeDate=None, afterDate=None):
	table = 'T_ECONOMIC_DATA_WORDS_HISTORY'
	whereColumns = ['int_word_master_id']
	whereValues = [wordSeriesID]
	selectColumns = ['dt_data_time','flt_data_value']
	
	if beforeDate is not None:
		lessThanColumns = ['dt_data_time']
		lessThanValues = [beforeDate]
	else:
		lessThanColumns = None
		lessThanValues = None
	if afterDate is not None:
		moreThanColumns = ['dt_data_time']
		moreThanValues = [afterDate]
	else:
		moreThanColumns = None
		moreThanValues = None	

	statement = generateSelectStatement(table, 
										whereColumns=whereColumns, whereValues=whereValues, 
										lessThanColumns=lessThanColumns, lessThanValues=lessThanValues,
										moreThanColumns=moreThanColumns, moreThanValues=moreThanValues,
										selectColumns=selectColumns,
										orderBy=(['dt_data_time'], 'ASC'))
	return statement

def getWordHistory_WordHistoryTable(conn, cursor, wordSeriesID, beforeDate=None, afterDate=None):
	statement = __get_wordHistory_WordHistoryTable_Statement(wordSeriesID, beforeDate=beforeDate, afterDate=afterDate)
	cursor.execute(statement)
	series = cursor.fetchall()
	return series

########################################Specific Query Construction Code / Find Word Series ID 
def __get_retrieve_WordSeriesID_Statement(dataSeriesID, wordType, wordSubType, wordSuperType, wordTicker=None):
	table = 'T_ECONOMIC_DATA_WORDS'
	selectColumns = ['int_word_master_id']
	whereColumns = ['int_data_master_id', 'code_word_type', 'code_word_subtype', 'code_word_supertype']
	whereValues = [dataSeriesID, wordType, wordSubType, wordSuperType]
	return generateSelectStatement(	table, 
									whereColumns=whereColumns, 
									whereValues=whereValues, 
									selectColumns=selectColumns)

def retrieve_WordSeriesID(conn, cursor, dataSeriesID, wordType, wordSubType, wordSuperType, wordTicker=None, insertIfNot=False):
	statement = __get_retrieve_WordSeriesID_Statement(dataSeriesID, wordType, wordSubType, wordSuperType, wordTicker=wordTicker)
	cursor.execute(statement)
	seriesID = cursor.fetchone()
	wordSeriesString = 	'{0} (Series ID {1}, type {2}, subtype {3}, supertype {4})'\
						.format(wordTicker, dataSeriesID, wordType, wordSubType, wordSuperType)
	if seriesID is None and insertIfNot:
		log.info('Series ID Not Found for %s... Creating.', wordSeriesString)
		# Add SeriesID, Name, Ticker to T_ECONOMIC_DATA_WORDS
		table = 'T_ECONOMIC_DATA_WORDS'
		columns = ['int_data_master_id', 'code_word_type', 'code_word_subtype', 'code_word_supertype']
		values = [dataSeriesID, wordType, wordSubType, wordSuperType]
		if wordTicker is not None:
			columns.append('txt_word_ticker')
			values.append(wordTicker)
		statement = generateInsertStatement(table, columns, values)			
		cursor.execute(statement)
		conn.commit()
		seriesID = cursor.lastrowid
		log.info('Series ID Created for %s: (%d)', wordSeriesString, seriesID)
		return seriesID
	log.info('Series ID Found for %s: (%d).', wordSeriesString, seriesID[0])
	return int(seriesID[0])	

########################################Specific Query Construction Code / Find All Word Series IDs 
def retrieve_AllWordSeriesIDs(conn, cursor, wordTypes=None, wordSubTypes=None, wordTicker=None):
	table = 'T_ECONOMIC_DATA_WORDS'
	selectColumns = ['int_word_master_id']
	if wordTypes is not None or wordSubTypes is not None:
		whereColumns = []
		whereValues = []
		if wordTypes is not None:
			whereColumns.append('code_word_type')
			whereValues.append(wordTypes)
		if wordSubTypes is not None:
			whereColumns.append('code_word_subtype')
			whereValues.append(wordSubTypes)
	else:
		whereColumns = None
		whereValues = None

	statement = generateSelectStatement(table, 
										whereColumns=whereColumns, 
										whereValues=whereValues, 
										selectColumns=selectColumns)
	cursor.execute(statement)
	seriesIDs = [x[0] for x in cursor.fetchall()]
	return seriesIDs


########################################Database Helper Code 
########################################Database Helper Code / Atomic Commit
def commitDBStatement(conn, cursor, statement):
	try:
		log.debug('\t\t\tAttempting %s ...', statement)
		cursor.execute(statement)
		conn.commit()
		log.debug('\t\t\t...Succeeded')
		return (True, "")
	except Exception as e:
		log.error('\t\t\t %s Failed!', statement)
		log.error('%s', e)
		return (False, str(e))

########################################Database Helper Code / Outward-Facing Simple Statement Executor
def executeSimpleDatabaseStatement(statement, showCols=True, databaseFile=defaultDB):
	# Open DB Connection
	conn = sq.connect(databaseFile)
	c = conn.cursor()
	try:
		statement = statement.replace('\t', '')
		statement = statement.replace('\n', '')
		statement = statement.strip()
		queryType = lower(statement[0:find(statement,' ')])
		#TODO: Fix to ignore whitespace/handle it better. Consider using regexes.
		if queryType == 'select':
			if showCols:
				tableNameStart = find(statement,'from ') + 5
				tableNameEnd = find(statement,' ',tableNameStart)
				if tableNameEnd == -1: tableNameEnd = len(statement)
				tableName = statement[tableNameStart:tableNameEnd]
				pragmaCommand = 'pragma table_info("' + tableName + '")'
				c.execute(pragmaCommand)
				pragma = c.fetchall()
				print 'COLUMN NAMES:'
				print [col[1] for col in pragma]
				print 'COLUMN TYPES:'	
				print [col[2] for col in pragma]
			print 'VALUES:'
			c.execute(statement)
			results = c.fetchall()
			if len(results) == 0:
				print 'NOTHING FOUND'
			else:
				pprint(results)
		elif queryType == 'insert' or queryType == 'update':
			c.execute(statement)
			c.commit()
		else:
			raise NameError('simple database query type not recognized')
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

########################################Database Helper Code / Outward-facing Table Peeking for Debugging
def peerIntoDatabase(tableName, databaseFile=defaultDB, displayPragma=False, limit=10):
	# Open DB Connection
	conn = sq.connect(databaseFile)
	c = conn.cursor()
	try:
		# Print Table Length
		viewLengthCommand = 'select count(*) from ' + tableName
		c.execute(viewLengthCommand)
		print 'There are ' + str(c.fetchall()[0][0]) + ' rows in ' + tableName
		# Get Table Column Names and Types
		pragmaCommand = 'pragma table_info("' + tableName + '")'
		c.execute(pragmaCommand)
		pragma = c.fetchall()
		#		(pragma columns are as follows:)
		#		(cid,name,type,notnull,dflt_value,pk)
		columnNames = [col[1] for col in pragma]
		columnTypes = [col[2] for col in pragma]
		if displayPragma:
			print '(pragma columns are as follows:)'
			print '(column-id,name,type,not-null,default-value,primary-key)'
			pprint(pragma)
		# Print First Few Rows of Table		
		viewFirstFewCommand = 'select * from ' + tableName + ' limit ' + str(limit)
		c.execute(viewFirstFewCommand)
		firstFew = c.fetchall()
		print columnTypes
		print columnNames
		pprint(firstFew)
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

def _databasePicker():
	print '\n\nChoose DB:'
	print '\tCurrent Database is {0}'.format(defaultDB)
	newDB = raw_input('\tPlease enter a database name if you desire:  ')
	return (newDB if len(newDB)>0 else defaultDB)

def _tablePicker(databaseFile):
	conn = sq.connect(databaseFile)
	c = conn.cursor()
	try:
		print '\n\nChoose Table or press enter to skip:'
		viewTableCommand = "select name from sqlite_master where type = 'table'"
		c.execute(viewTableCommand)
		tables = c.fetchall()
		print '\tCurrent Tables Are: '
		for t in tables: print '\t\t{0}'.format(t[0])
		return raw_input('\tPlease enter a table name:  ')
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

def _commandReader(databaseFile):
	conn = sq.connect(databaseFile)
	c = conn.cursor()
	try:
		while True:
			statement = raw_input('\n\tType Any Command, or "!q" to quit. Finish a command in "!r" to skip.:\n')
			if statement == '!q':
				break
			if statement == '' or statement[-2:]=='!r':
				continue
			else:
				try:
					executeSimpleDatabaseStatement(statement, showCols=True, databaseFile=databaseFile)
				except Exception as e:
					print 'ERROR: '
					print e

	except:
		raise
	finally:
		# Close DB Connection
		conn.close()


if __name__ == '__main__':
	db = _databasePicker()
	table = _tablePicker(db)
	if table != '':
		peerIntoDatabase(table, db, displayPragma=True)
	_commandReader(db)
