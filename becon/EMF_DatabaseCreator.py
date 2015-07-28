import os
import sys
import sqlite3 as sq
import EMF_util as EM_util

fullTableCreationInstructions = {}
DataNameDataTableLink = {}

createDataInformationTable = '''
CREATE TABLE T_ECONOMIC_DATA
(
int_data_master_id INTEGER UNIQUE NOT NULL PRIMARY KEY,
txt_data_name TEXT,
txt_data_ticker TEXT UNIQUE,
code_data_supertype INTEGER DEFAULT 0,
code_data_type INTEGER DEFAULT 0,
code_data_subtype INTEGER DEFAULT 0,
int_data_periodicity INTEGER, /* periodicity in 1/year. Days is 365; weeks is 52; months is 12; quarters is 4 */
dt_earliest_value INTEGER, /* earliest data point we have downloaded */
dt_latest_value INTEGER, /* latest data point we have */
dt_last_updated_history INTEGER, /* when we got the historical data last */
dt_last_updated_metadata INTEGER, /* when we got the historical data last */
int_unsuccessful_inserts INTEGER DEFAULT 0
);
'''
fullTableCreationInstructions['T_ECONOMIC_DATA'] = createDataInformationTable

EconomicDataKeys = [
'txt_data_name',
'txt_data_ticker',
'code_data_supertype',
'code_data_type',
'code_data_subtype',
'int_data_periodicity',
'dt_earliest_value',
'dt_latest_value', 
'dt_last_updated_history',
'dt_last_updated_metadata',
'int_unsuccessful_inserts']
EconomicDataDict = dict.fromkeys(EconomicDataKeys,'T_ECONOMIC_DATA')
DataNameDataTableLink.update(EconomicDataDict)

createDetailedDataInformationTable = '''
CREATE TABLE T_ECONOMIC_DATA_DETAILED
(
int_data_master_id INTEGER UNIQUE NOT NULL PRIMARY KEY,
dt_last_updated_SOURCE INTEGER, /* last update according to SOURCE (e.g FRED) */
dt_earliest_value_SOURCE INTEGER, /* earliest date possible according to SOURCE (e.g FRED) */
dt_latest_value_SOURCE INTEGER, /* latest date possible according to SOURCE (e.g FRED) */
bool_generated_datapoint INTEGER,
bool_is_normalized INTEGER,
bool_is_seasonally_adjusted INTEGER,
bool_is_real INTEGER,
bool_is_deflator INTEGER,
code_is_gross INTEGER, 	/* is it gross (1) or net (0)? [Leaving open for more values] */
code_is_level INTEGER, 	/* does the level of the value matter (1) as well as the rate of change? [Leaving open for more values] */
code_good_direction INTEGER DEFAULT 0, 	/* is positive 'good'? (1) or is negative 'good' (-1)? [Use sparingly when it matters] */
code_private_public INTEGER,
code_economic_activity INTEGER,
code_data_adjustment INTEGER,
code_sector INTEGER,
code_data_units INTEGER,
code_item_type INTEGER
);
'''
fullTableCreationInstructions['T_ECONOMIC_DATA_DETAILED'] = createDetailedDataInformationTable

EconomicDataDetailedKeys = [
'dt_last_updated_SOURCE',
'dt_earliest_value_SOURCE',
'dt_latest_value_SOURCE',
'bool_generated_datapoint',
'bool_is_normalized',
'bool_is_seasonally_adjusted',
'bool_is_real',
'bool_is_deflator',
'code_is_gross',
'code_is_level',
'code_good_direction',
'code_private_public',
'code_economic_activity',
'code_data_adjustment',
'code_sector',
'code_data_units',
'code_item_type']
EconomicDataDetailedDict = dict.fromkeys(EconomicDataDetailedKeys,'T_ECONOMIC_DATA_DETAILED')
DataNameDataTableLink.update(EconomicDataDetailedDict)

createExtraneousDataTable = '''
CREATE TABLE T_ECONOMIC_DATA_EXTRANEOUS
(
int_data_master_id INTEGER UNIQUE NOT NULL PRIMARY KEY,
txt_data_source TEXT,
txt_data_original_source TEXT,
txt_units_original TEXT,
txt_notes TEXT,
FOREIGN KEY (int_data_master_id) REFERENCES T_ECONOMIC_DATA (int_data_master_id)
);
'''
fullTableCreationInstructions['T_ECONOMIC_DATA_EXTRANEOUS'] = createExtraneousDataTable

EconomicDataExtraneousKeys = [
'txt_data_source',
'txt_data_original_source',
'txt_units_original',
'txt_notes']
EconomicDataExtraneousDict = dict.fromkeys(EconomicDataExtraneousKeys,'T_ECONOMIC_DATA_EXTRANEOUS')
DataNameDataTableLink.update(EconomicDataExtraneousDict)

createDataHistoryTable = '''
CREATE TABLE T_ECONOMIC_DATA_HISTORY
(
int_data_master_id INTEGER,
dt_data_time INTEGER,
flt_data_value REAL,
bool_is_interpolated INTEGER DEFAULT 0,
bool_is_forecast INTEGER DEFAULT 0,
UNIQUE (int_data_master_id, dt_data_time) ON CONFLICT REPLACE,
FOREIGN KEY (int_data_master_id) REFERENCES T_ECONOMIC_DATA (int_data_master_id)
);
'''
fullTableCreationInstructions['T_ECONOMIC_DATA_HISTORY'] = createDataHistoryTable

createWordDataInformationTable = '''
CREATE TABLE T_ECONOMIC_DATA_WORDS
(
int_word_master_id INTEGER UNIQUE NOT NULL PRIMARY KEY,
int_data_master_id INTEGER NOT NULL,
code_word_supertype INTEGER NOT NULL,
code_word_type INTEGER NOT NULL,
code_word_subtype INTEGER NOT NULL,
txt_word_ticker TEXT UNIQUE,
int_word_periodicity INTEGER, /* periodicity in 1/year. Days is 365; weeks is 52; months is 12; quarters is 4 */
dt_earliest_word INTEGER, /* earliest data point we have generated */
dt_latest_word INTEGER, /* latest data point we have generated*/
dt_last_generated INTEGER, /* when we generated data last */
int_unsuccessful_generations INTEGER DEFAULT 0,
UNIQUE (int_data_master_id, code_word_supertype, code_word_type, code_word_subtype) ON CONFLICT REPLACE,
FOREIGN KEY (int_data_master_id) REFERENCES T_ECONOMIC_DATA (int_data_master_id)
);
'''
fullTableCreationInstructions['T_ECONOMIC_DATA_WORDS'] = createWordDataInformationTable

EconomicDataWordKeys = [
'int_data_master_id', # Normally we wouldn't put in repeated values in EconDataDict, but this is the first time. It's fine. Can't do it again though.
'code_word_supertype',
'code_word_type',
'code_word_subtype',
'txt_word_ticker',
'int_word_periodicity',
'dt_earliest_word',
'dt_latest_word', 
'dt_last_generated',
'int_unsuccessful_generations']
EconomicDataDict = dict.fromkeys(EconomicDataWordKeys,'T_ECONOMIC_DATA_WORDS')
DataNameDataTableLink.update(EconomicDataDict)

createDataWordsHistoryTable = '''
CREATE TABLE T_ECONOMIC_DATA_WORDS_HISTORY
(
int_word_master_id INTEGER,
dt_data_time INTEGER,
flt_data_value REAL,
PRIMARY KEY (int_word_master_id, dt_data_time),
FOREIGN KEY (int_word_master_id) REFERENCES T_ECONOMIC_DATA_WORDS (int_word_master_id)
);
'''
fullTableCreationInstructions['T_ECONOMIC_DATA_WORDS_HISTORY'] = createDataWordsHistoryTable



manualTableCreationInstructions = {
			# 'T_ECONOMIC_DATA':createDataInformationTable,
			# 'T_ECONOMIC_DATA_DETAILED':createDetailedDataInformationTable,
			# 'T_ECONOMIC_DATA_EXTRANEOUS':createExtraneousDataTable,
			# 'T_ECONOMIC_DATA_HISTORY':createDataHistoryTable,
			# 'T_ECONOMIC_DATA_WORDS':createWordDataInformationTable,
			# 'T_ECONOMIC_DATA_WORDS_HISTORY':createDataWordsHistoryTable,
		}

def get_TableExists_Statement(tableName):
	return 'select name from sqlite_master where type="table" and name = "' + tableName + '";'

def get_DropTable_Statement(tableName):
	return 'drop table if exists ' + tableName + ';'

def checkIfDBExistsWithTables(DBFilePath=EM_util.defaultDB, manual=False):
	# Check if DB File Exists
	if not os.path.isfile(DBFilePath):
		return False

	# Find List of DBs to Create
	if manual:
		tableCreationInstructions = manualTableCreationInstructions
	else:
		tableCreationInstructions = fullTableCreationInstructions

	conn = sq.connect(DBFilePath)
	c = conn.cursor()
	# Check for Existence of Individual Tables
	try:
		for (tableName, instruction) in tableCreationInstructions.iteritems():
			statement = get_TableExists_Statement(tableName)
			print "Checking existence of "  + tableName + "...",
			c.execute(statement)
			if c.fetchall()[0][0] == 0:
				print "..." + tableName + " doesn't exist."
				return False
			print "... Exists!"
		print "DB exists with appropriate tables"
		return True
	except:
		raise
	finally:
		conn.close()


def doOneTimeDBCreation(force=False, DBFilePath=EM_util.defaultDB, manual=False):
	# Find List of DBs to Create
	print 'Performing {0} table creation'.format('partial' if manual else 'full')
	if manual:
		tableCreationInstructions = manualTableCreationInstructions
	else:
		tableCreationInstructions = fullTableCreationInstructions

	# Find Database Directory
	directory = os.path.dirname(DBFilePath)
	# Create Directory if Necessary
	if not os.path.exists(directory):
		print 'Creating OS Directory'
		os.makedirs(directory)
	# Connect to Database
	conn = sq.connect(DBFilePath)
	c = conn.cursor()
	try:
		for (tableName, instruction) in tableCreationInstructions.iteritems():
			# Find List of DBs to Drop, if Necessary
			if force:
				statement = get_DropTable_Statement(tableName)
				print "Dropping "  + tableName + "...",
				c.execute(statement)
				conn.commit()
				print "... Dropped!"
			summaryString = instruction[1:instruction.find('\n',3)]
			print "Executing "  + summaryString,
			c.execute(instruction)
			conn.commit()
			print "... Executed!"
	except:
		print "... Failed!"
		raise
	finally:
		conn.close()

if __name__ == '__main__':
	# Read Arguments
	force = len(sys.argv) > 1 and '-f' in sys.argv[1:]
	if len(sys.argv) > 1 and '-t' in sys.argv[1:]:
		DBFilePath=EM_util.testDB
	else:
		DBFilePath=EM_util.defaultDB
	manual = len(sys.argv) > 1 and '-m' in sys.argv[1:]
	# Do DB Creation
	try:
		print 'Creating ' + DBFilePath
		doOneTimeDBCreation(force=force,DBFilePath=DBFilePath,manual=manual)
	except sq.OperationalError as e:
		print 'Database Creation Failed.'
		if 'already exists' in str(e):
			print '\tDatabases already exist. \n\tIf overwriting was your intent, use -f flag to force creation.'
		else:
			raise e
