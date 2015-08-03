
def resetTestSuite(failGracefully=False):
	global TestNum
	global FailGracefully
	TestNum = 1
	FailGracefully = failGracefully

def simpleAssertionTest(assertionToTry, failureText, throwawayTest=False):
	try:
		assert assertionToTry, failureText
		if not throwawayTest:
			global TestNum
			print 'TEST {0} OK: {1}'.format(TestNum, failureText)
			TestNum += 1
		else: 
			print 'TEST OK: {0}'.format(failureText)
	except AssertionError:
		global FailGracefully
		if FailGracefully:
			print 'TEST {0} FAILED: {1}'.format(TestNum, failureText)
			TestNum += 1
			return
		else:
			assert assertionToTry, failureText # This will fail and stop execution.


 # TODO: Fix! (define new type of error)
 # 		This shouldn't deal in assertion errors. 
 # 		(If ErrorType throw in fn is AssertionError, this will allow tests to pass where they shouldn't)
 # 		Define a new type of error that has same functionality as AssertionError, but isn't named such
def tryExceptTest(fnToTry, failureText, expectToFail=True, ErrorType=Exception):
	global TestNum
	global FailGracefully
	try:
		fnToTry()
		assert (not expectToFail), failureText
	except ErrorType:
		if expectToFail:
			print 'TEST {0} OK: {1}'.format(TestNum, failureText)
			TestNum += 1
			return
		else:
			if FailGracefully:
				print 'TEST {0} FAILED: {1}'.format(TestNum, failureText)
				TestNum += 1
				return
			else:
				assert False, failureText # This will fail and stop execution.
	except:
		if FailGracefully:
			print 'TEST {0} FAILED (WARNING! UNEXPECTED ERROR!): {1}'.format(TestNum, failureText)
			TestNum += 1
			return
		else:
			assert False, 'Unexpected Error Type' # This will fail and stop execution.

def compareFloatValues(num1, num2, numDigits=6):
	exp = 10**numDigits
	return round(num1*exp)==round(num2*exp)


