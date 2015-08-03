# For when you want to say a test passed
def outputTestOK(description, throwawayTest=False):
	global TestNum
	if throwawayTest:
		print 'TEST\t\tOK:\t{1}'.format(TestNum, description)
	else:
		print 'TEST\t{0}\tOK:\t{1}'.format(TestNum, description)
		TestNum += 1
	return True

# For when you want to say a test failed
# TODO: Use unexpectedError
def outputTestFAILED(description, unexpectedError=False): 
	global TestNum
	global FailGracefully
	if FailGracefully and (not unexpectedError):
		print 'TEST\t{0}\tFAILED:\t{1}'.format(TestNum, description)
		TestNum += 1
		return False
	else:
		# This will fail and stop execution.
		print 'TEST\t{0}\tFAILED:\t{1}'.format(TestNum, description)
		raise Exception('Test Failed Unexpectedly')

def resetTestSuite(failGracefully=False):
	global TestNum
	global FailGracefully
	TestNum = 1
	FailGracefully = failGracefully

def simpleAssertionTest(assertionToTry, failureText, throwawayTest=False):
	try:
		assert assertionToTry, failureText
		return outputTestOK(failureText, throwawayTest=throwawayTest)
	except AssertionError:
		return outputTestFAILED(failureText, unexpectedError=False)
	except:
		outputTestFAILED(failureText, unexpectedError=True)
		
 # TODO: Fix! (define new type of error)
 # 		This shouldn't deal in assertion errors. 
 # 		(If ErrorType throw in fn is AssertionError, this will allow tests to pass where they shouldn't)
 # 		Define a new type of error that has same functionality as AssertionError, but isn't named such
 # TODO: Fix this all! It got FUBARed when you tried to incorporate graceful failures.
 #		e.g. the assertion below fnToTry() will be caught by the general exception at the bottom,
 #			producing unexpected output
def tryExceptTest(fnToTry, failureText, expectToFail=True, ErrorType=Exception, throwawayTest=False):
	try:
		fnToTry()
		assert (not expectToFail), failureText
	except ErrorType:
		if expectToFail:
			return outputTestOK(failureText, throwawayTest=throwawayTest)
		else:
			return outputTestFAILED(failureText, unexpectedError=False)
	except AssertionError:
		return outputTestFAILED(failureText, unexpectedError=False)		
	except:
		return outputTestFAILED(failureText, unexpectedError=True)

def floatComparisonTest(num1, num2, label1, label2, testDescription, numDigits=6, throwawayTest=False):
	description = '{0}=={1}: {2}=={3}: {4}'.format(num1, num2, label1, label2, testDescription)
	if __compareFloatValues(num1, num2, numDigits=numDigits):
		return outputTestOK(description, throwawayTest=throwawayTest)
	else:
		return outputTestFAILED(failureText, unexpectedError=False)

def __compareFloatValues(num1, num2, numDigits=6):
	exp = 10**numDigits
	return round(num1*exp)==round(num2*exp)


