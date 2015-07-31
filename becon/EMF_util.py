#TODO: Fill in LEVEL MATTERS column in FRED CSV
#TODO: Fill in POSITIVE IS GOOD column in FRED CSV
import logging as log
import datetime
import os
import re
import warnings

from calendar import monthrange

HomeDirectory = os.path.expanduser('~') + '/DataModeling/'

######################## DATABASE CODE
DBRepository = HomeDirectory + 'database/'
ProdDBFilePath = DBRepository + 'prod_economicData.db'
QADBFilePath = DBRepository + 'qa_economicData.db'
TestDBFilePath = DBRepository + 'test_economicData.db'

testDB = TestDBFilePath
defaultDB = TestDBFilePath

######################## LOGGING CODE
LogRepository = HomeDirectory + 'log/'
TestLogFilePath = LogRepository + 'test.log'
DeletableLogFilePath = LogRepository + 'deletableLog.log'
defaultLog = DeletableLogFilePath

# TODO: Implement Options, Log channeling
def initializeLog(recordLog=False, clearRecorded=True, recordLevel=None, logFilePath=None):
	if recordLog:
		fileMode = 'wb' if clearRecorded else 'ab'
		recordLevel = log.INFO if recordLevel is None else recordLevel
		logFilePath = defaultLog if logFilePath is None else logFilePath
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


######################## Website CODE
webJSONRepository = HomeDirectory + 'website/json/'
webJSONRawSeriesRepository = webJSONRepository + 'raw_series/'
webJSONAvailableDataFile = webJSONRawSeriesRepository + 'availableData.json'

rawSeriesFormatTYpes = {
	'rickshawJSON': ('rs', '.json'),
	'd3JSON': ('d3', '.json')
}

######################## Data Loading CODE
loadJSONRepository = HomeDirectory + 'dataloader/json/'
loadCSVRepository = HomeDirectory + 'dataloader/csv/'

######################## FRED CODE
FREDSeriesCSV = loadCSVRepository + 'FredSeries.csv'
FREDAPIKey = '58e35f2aed62d638d589228471673b79'

FREDSeriesCSVCols = {
	'TICKER_COL' : 1,
	'METADATA_STATUS_COL' : 2,
	'METADATA_TIMESTAMP_COL' : 3,
	'HISTORY_STATUS_COL' : 4,
	'HISTORY_TIMESTAMP_COL' : 5,
	'TYPE_COL' : 7,
	'SUPERTYPE_COL' : 8,
	'SUBTYPE_COL' : 9,
	'IMPORTANCE_COL' : 10,
	'IS_LEVEL_COL' : 11,
	'POSITIVE_IS_GOOD_COL' : 12,
	'FORCE_METADATA_REDOWNLOAD_COL' : 13,
	'FORCE_HISTORY_REDOWNLOAD_COL' : 14
}

FREDSeriesCSVTimeFormat = '%Y-%m-%d %H:%M:%S'

FREDDownloadDelayHistory = datetime.timedelta(days=0.5)
FREDDownloadDelayMetadata = datetime.timedelta(days=5)
FREDSkipDownload = '-1'
FREDForceDownload = '1'

######################## DATATYPE/WORDTYPE CODE
wordTypes = {
	'timeToRecession' : 2,
	'1OD' : 3,
}
wordSuperTypes = {
	'generic' : 1,
	'recessionSignal' : 2,
}
wordSubTypes = {
	'NotApplicable' : 1,
}

dataSupertypes = {
	'unknown' : 0,
	'generic' : 1,
	'RecessionIndicator' : 2,
}
dataTypes = {
	'unknown' : 0,
	'generic' : 1,
	'Commodities' : 2,
	'Consumption' : 3,
	'Employment' : 4,
	'Financial' : 5,
	'Inflation' : 6,
	'Monetary' : 7,
	'Prices' : 8,
	'Production': 9,
	'RecessionIndicator' : 10,
	'Technology' : 11,
	'PotentialSpending' : 12,
}
dataSubtypes = {
	'unknown' : 0,
	'generic' : 1,
	'Automotive' : 2,
	'Consumers' : 3,
	'DurableGoods' : 4,
	'E-Commerce' : 5,
	'HoursWorked' : 6,
	'Housing' : 7,
	'HousingTurnover' : 8,
	'Income' : 9,
	'InterestRates' : 10,
	'Inventories' : 11,
	'LaborTurnover' : 12,
	'Manufacturing' : 13,
	'MoneySupply' : 14,
	'NonDurableGoods' : 15,
	'NonManufacturing' : 16,
	'NumberWorking' : 17,
	'Oil' : 18,
	'Potential' : 19,
	'Producers' : 20,
	'RecessionIndicator' : 21,
	'Saving' : 22,
	'Services' : 23,
	'Stocks' : 24,
	'Unemployment' : 25,
}
levelMattersTypes = {
	'unknown' 	: 0,
	'yes' 		: 1,
}
goodDirectionTypes = {
	'unknown' 	: 0,
	'1' 		: 1,  # Positive is 'good'
	'-1' 		: -1, # Negative is 'good'	
}

######################## DIRECTORY MANAGEMENT CODE
AllDirectories = [DBRepository, LogRepository, webJSONRepository, webJSONRawSeriesRepository,loadJSONRepository, loadCSVRepository]

def createDirectories():
	for directory in AllDirectories:
		if not os.path.exists(directory):
			os.makedirs(directory)

######################## DATE TIME CODE
def dtGetNowAsEpoch():
	dt = datetime.datetime.now()
	return (dt-datetime.datetime(1970,1,1)).total_seconds()

def dtConvert_YYYY_MM_DD_TimetoEpoch(timeString):
	dt = datetime.datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S-%f')  
	return (dt-datetime.datetime(1970,1,1)).total_seconds()

def dtConvert_YYYY_MM_DDtoEpoch(timeString):
	dt = datetime.datetime.strptime(timeString, '%Y-%m-%d')
	return (dt-datetime.datetime(1970,1,1)).total_seconds()

def dtConvert_Mmm_YtoEpoch(timeString,endOfMonth=True):
	dt = datetime.datetime.strptime(timeString, '%b-%y')
	if endOfMonth:
		dt = dt.replace(day = monthrange(dt.year,dt.month)[1])
	return (dt-datetime.datetime(1970,1,1)).total_seconds()

def dtConvert_EpochtoY_M_D(epochTime):
	return strftime(datetime.datetime.fromtimestamp(epochTime), '%Y-%m-%d', force=True)

# Taken from StackOverflow
def strftime(datetime_, format, force=False):
    """`strftime()` that works for year < 1900.

    Disregard calendars shifts.

    >>> def f(fmt, force=False):
    ...     return strftime(datetime(1895, 10, 6, 11, 1, 2), fmt, force)
    >>> f('abc %Y %m %D') 
    'abc 1895 10 10/06/95'
    >>> f('%X')
    '11:01:02'
    >>> f('%c') #doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ValueError: '%c', '%x' produce unreliable results for year < 1900
    use force=True to override
    >>> f('%c', force=True)
    'Sun Oct  6 11:01:02 1895'
    >>> f('%x') #doctest:+NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ValueError: '%c', '%x' produce unreliable results for year < 1900
    use force=True to override
    >>> f('%x', force=True)
    '10/06/95'
    >>> f('%%x %%Y %Y')
    '%x %Y 1895'
    """
    year = datetime_.year
    if year >= 1900:
       return datetime_.strftime(format)

    # mMke year larger then 1900 using 400 increment
    # (Dates repeat every 400 years)
    assert year < 1900
    factor = (1900 - year - 1) // 400 + 1
    future_year = year + factor * 400
    assert future_year > 1900

    format = Specifier('%Y').replace_in(format, year)
    result = datetime_.replace(year=future_year).strftime(format)
    if any(f.ispresent_in(format) for f in map(Specifier, ['%c', '%x'])):
        msg = "'%c', '%x' produce unreliable results for year < 1900"
        if not force:
            raise ValueError(msg + " use force=True to override")
        warnings.warn(msg)
        result = result.replace(str(future_year), str(year))
    assert (future_year % 100) == (year % 100) # last two digits are the same
    return result

# Taken from StackOverflow
class Specifier(str):
    """Model %Y and such in `strftime`'s format string."""
    def __new__(cls, *args):
        self = super(Specifier, cls).__new__(cls, *args)
        assert self.startswith('%')
        assert len(self) == 2
        self._regex = re.compile(r'(%*{0})'.format(str(self)))
        return self

    def ispresent_in(self, format):
        m = self._regex.search(format)
        return m and m.group(1).count('%') & 1 # odd number of '%'

    def replace_in(self, format, by):
        def repl(m):
            n = m.group(1).count('%')
            if n & 1: # odd number of '%'
                prefix = '%'*(n-1) if n > 0 else ''
                return prefix + str(by) # replace format
            else:
                return m.group(0) # leave unchanged
        return self._regex.sub(repl, format)

######################## MAIN
if __name__ == '__main__':
	args = sys.argv[1:]
	if len(args) > 0:
		if '-createDirs' in args:
			createDirectories()
