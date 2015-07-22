import logging as log
import os

HomeDirectory = os.path.expanduser('~') + '/DataModeling'

######################## DATABASE CODE
DBRepository = HomeDirectory + '/Databases/EconModeling/'
ProdDBFilePath = DBRepository + 'prod_economicData.db'
QADBFilePath = DBRepository + 'qa_economicData.db'
TestDBFilePath = DBRepository + 'test_economicData.db'

testDB = TestDBFilePath
defaultDB = TestDBFilePath

######################## LOGGING CODE
LogRepository = HomeDirectory + '/Logging/EconModeling/'
TestLogFilePath = LogRepository + 'test.log'
testLog = LogRepository + 'deletableLog.log'

# TODO: Implement Options, Log channeling
def initializeLog(recordLog=False, clearRecorded=True, recordLevel=None, logFilePath=testLog):
	if recordLog:
		fileMode = 'wb' if clearRecorded else 'ab'
		recordLevel = log.INFO if recordLevel is None else recordLevel
		log.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', 
						datefmt='%m/%d/%Y %I:%M:%S %p',
						filename=logFilePath,
						filemode=fileMode,
						level=recordLevel)	
	else:
		recordLevel = log.DEBUG if recordLevel is None else recordLevel
		log.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', 
						datefmt='%I:%M:%S %p',
						level=recordLevel)			


######################## JSON CODE
JSONRepository = HomeDirectory + '/website/json/'
JSONRawSeriesRepository = HomeDirectory + '/website/json/raw_series'
JSONAvailableDataFile = JSONRepository + 'availableData.json'

######################## CSV CODE
CSVRepository = HomeDirectory + '/csv/'

######################## FRED CODE
FREDSeriesCSV = CSVRepository + 'FredSeries.csv'
FREDAPIKey = '58e35f2aed62d638d589228471673b79'

######################## DIRECTORY MANAGEMENT CODE
AllDirectories = [DBRepository, LogRepository, JSONRepository, JSONRawSeriesRepository]

def createDirectories():
	for directory in AllDirectories:
		if not os.path.exists(directory):
			os.makedirs(directory)

######################## DATE TIME CODE
def dtGetNowAsEpoch():
	dt = datetime.datetime.now()
	return int(dt.strftime('%s'))	

def dtConvert_YYYY_MM_DD_TimetoEpoch(timeString):
	dt = datetime.datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S-%f')  
	return int(dt.strftime('%s'))

def dtConvert_YYYY_MM_DDtoEpoch(timeString):
	dt = datetime.datetime.strptime(timeString, '%Y-%m-%d')
	return int(dt.strftime('%s'))

def dtConvert_Mmm_YtoEpoch(timeString,endOfMonth=True):
	dt = datetime.datetime.strptime(timeString, '%b-%y')
	if endOfMonth:
		dt = dt.replace(day = monthrange(dt.year,dt.month)[1])
	return int(dt.strftime('%s'))

def dtConvert_EpochtoY_M_D(epochTime):
	return datetime.datetime.fromtimestamp(epochTime).strftime('%Y-%m-%d')

######################## MAIN
if __name__ == '__main__':
	args = sys.argv[1:]
	if len(args) > 0:
		if '-createDirs' in args:
			createDirectories()
