from operator import itemgetter
from cluster_util import SymmetricTable_Sparse, SymmetricTable_FixedSize
import test_util as testing

# TODO: look into unittest module
def testSymmetricTableSparse():
	# Test Order Matters
	table = SymmetricTable_Sparse() #orderMatters=True
	testing.tryExceptTest(lambda: table.get(1,2), 'Should not get unset value.', expectToFail=True, ErrorType=KeyError)
	testing.simpleAssertionTest(lambda: table.get(1, 2, 'a') == 'a', 'Default return for Get test passed.')	

	table.set(1,2,'a')
	testing.simpleAssertionTest(lambda: table.get(1, 2) == 'a', 'Get value test passed.')	
	testing.tryExceptTest(lambda: table.get(2,1), 'Should not get values in reversed order.', expectToFail=True, ErrorType=KeyError)

	testing.simpleAssertionTest(lambda: table.delete(1,2) == 'a', 'Current value should return on deletion.')
	testing.tryExceptTest(lambda: table.delete(2,1), 'Should get exception when deleting nonexistent value.', expectToFail=True, ErrorType=KeyError)
	testing.tryExceptTest(lambda: table.delete(2,1,silent=True), 'Should not get exception when deleting nonexistent value silently.', expectToFail=False, ErrorType=Exception)
	testing.simpleAssertionTest(lambda: table.delete(1,2), 'Current value should return on deletion.')
	testing.simpleAssertionTest(lambda: table.delete(2,2,'a') == 'a', 'Default value should return on deletion of non-existent object.')

	# Test Order Doesn't Matter
	table = SymmetricTable_Sparse(orderMatters=False)
	table.set(1,2,'a')
	testing.simpleAssertionTest(lambda: table.get(1, 2) == 'a', 'Get value test passed.')
	testing.simpleAssertionTest(lambda: table.get(2, 1) == 'a', 'Symmetric Get test passed.')
	table.delete(2,1)
	testing.tryExceptTest(lambda: table.get(2,1), 'Should not get value after deletion.', expectToFail=True, ErrorType=KeyError)
	testing.tryExceptTest(lambda: table.get(1,2), 'Should not get value after deletion.', expectToFail=True, ErrorType=KeyError)

	# Test Sorting
	table = SymmetricTable_Sparse()
	x1 = [i for i in range(10)]
	x2 = [i for i in range(100,110)]
	x = zip(x1,x2)
	y = dict(zip(x, range(10,0,-1)))

	for a,b in y.iteritems():
		table.set(a[0], a[1], b)

	lmbda = lambda: sorted(y.items(), key=itemgetter(1)) == sorted(table.items(), key=itemgetter(1))
	testing.simpleAssertionTest(lmbda, 'table.items() works.')

# TODO: Move to own module
# TODO: look into unittest module
def testSymmetricTableFixed():
	tableSize = 10
	
	table = SymmetricTable_FixedSize(tableSize) #includeDiagonal=False
	testing.tryExceptTest(lambda: table.get(1,2), 'Should not get unset value.', expectToFail=True, ErrorType=KeyError)
	testing.simpleAssertionTest(lambda: table.get(1, 2, 'a') == 'a', 'Default value test passed.')
	testing.tryExceptTest(lambda: table.set(2,2,'a'), 'Should not set diagonal value.', expectToFail=True, ErrorType=AssertionError)

	# Make sure table is filling in order
	count = 0
	for row in range(tableSize):
		for col in range(row+1, tableSize):
			table.set(row, col, count)
			count += 1
			testing.simpleAssertionTest(lambda: len(table) == count, "Table entry didn't overwrite.", throwawayTest=True) # shouldn't work if we accidentally overwrite
	table.printTable()  # TODO: Make this into an actual test
	testing.simpleAssertionTest(lambda: count == tableSize*(tableSize-1)/2, "Table entry didn't skip.") # shouldn't work if we accidentally skip an entry

	table = SymmetricTable_FixedSize(tableSize, includeDiagonal=True)
	testing.tryExceptTest(lambda: table.set(2,2,'a'), 'Should be able to set diagonal value.', expectToFail=True, ErrorType=AssertionError)
	testing.simpleAssertionTest(lambda: table.delete(2,2) == 'a', 'Current value should return on deletion.')
	testing.tryExceptTest(lambda: table.delete(2,2,silent=True), 'Should fail to delete silently.', expectToFail=False, ErrorType=KeyError)
	testing.simpleAssertionTest(lambda: table.delete(2,2,'a') == 'a', 'Default value should return on deletion of non-existent object.')

	# Make sure table is filling in order
	count = 0
	for row in range(tableSize):
		for col in range(row, tableSize):
			table.set(row, col, count)
			count += 1
			testing.simpleAssertionTest(lambda: len(table) == count, "Table entry didn't overwrite.", throwawayTest=True) # shouldn't work if we accidentally overwrite
	table.printTable() # TODO: Make this into an actual test
	testing.simpleAssertionTest(lambda: count == tableSize*(tableSize+1)/2, "Table entry didn't skip.") # shouldn't work if we accidentally skip an entry

if __name__ == '__main__':
	testing.resetTestSuite()
	testSymmetricTableFixed()
	testSymmetricTableSparse()