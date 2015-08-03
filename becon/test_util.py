
def resetTestSuite():
	global TestNum
	TestNum = 1

def simpleAssertionTest(assertionToTry, failureText, throwawayTest=False):
	assert assertionToTry, failureText
	if not throwawayTest:
		global TestNum
		print 'TEST {0} OK: {1}'.format(TestNum, failureText)
		TestNum += 1
	else:
		print 'TEST OK: {0}'.format(failureText)


 # TODO: Fix! (define new type of angle)
 # 		This shouldn't deal in assertion errors. 
 # 		(i.e. if ErrorType throw in fn is AssertionError, this will allow tests to pass where they shouldn't)
 # 		Define a new type of error that has same functionality as AssertionError, but isn't named such
def tryExceptTest(fnToTry, failureText, expectToFail=True, ErrorType=Exception):
	global TestNum
	try:
		fnToTry()
		assert (not expectToFail), failureText
	except ErrorType:
		assert expectToFail, failureText
	except:
		assert False, 'Unexpected Error Type'
	print 'TEST {0} OK: {1}'.format(TestNum, failureText)
	TestNum += 1
