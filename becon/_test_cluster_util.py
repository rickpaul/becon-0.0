from operator import itemgetter
from cluster_util import SymmetricTable_Sparse, SymmetricTable_FixedSize

# TODO: look into unittest module
def testSymmetricTableSparse():
	# Test Order Matters
	table = SymmetricTable_Sparse()
	try:
		table.get(1,2)
		assert False, 'Should not get unset value.'
	except KeyError:
		assert True, 'Should not get unset value.'
	assert table.get(1, 2, 'a') == 'a', 'Default value test passed.'

	table.set(1,2,'a')
	assert table.get(1, 2) == 'a', 'Get value test passed.'
	try:
		table.get(2, 1)
		assert False, 'Should not get value in this order.'
	except KeyError:
		assert True, 'Should not get value in this order.'

	assert table.delete(1,2) == 'a'
	try:
		table.delete(1,2)
		assert False, 'Should get exception when deleting nonexistent value.'
	except KeyError:
		assert True, 'Should get exception when deleting nonexistent value.'
	try:
		table.delete(1,2,silent=True)
		assert True, 'Should not get exception when deleting nonexistent value.'
	except KeyError:
		assert False, 'Should not get exception when deleting nonexistent value.'
	assert table.delete(1,2,'a') == 'a', 'Default value deletion test passed.'

	# Test Order Doesn't Matter
	table = SymmetricTable_Sparse(orderMatters=False)
	table.set(1,2,'a')
	assert table.get(1, 2) == 'a', 'Get value test passed.'
	assert table.get(2, 1) == 'a', 'Get value test passed.'
	table.delete(2,1)
	try:
		table.get(2, 1)
		assert False, 'Should not get value after deletion.'
	except KeyError:
		assert True, 'Should not get value after deletion.'
	try:
		table.get(1, 2)
		assert False, 'Should not get value after deletion.'
	except KeyError:
		assert True, 'Should not get value after deletion.'

	# Test Sorting
	table = SymmetricTable_Sparse()
	x1 = [i for i in range(10)]
	x2 = [i for i in range(100,110)]
	x = zip(x1,x2)
	y = dict(zip(x, range(10,0,-1)))

	for a,b in y.iteritems():
		table.set(a[0], a[1], b)

	assert sorted(y.items(), key=itemgetter(1)) == sorted(table.items(), key=itemgetter(1))

# TODO: Move to own module
# TODO: look into unittest module
def testSymmetricTableFixed():
	tableSize = 10
	table = SymmetricTable_FixedSize(tableSize) #includeDiagonal=False
	try:
		table.get(1,2)
		assert False, 'Should not get unset value.'
	except KeyError:
		assert True, 'Should not get unset value.'
	assert table.get(1, 2, 'a') == 'a', 'Default value test passed.'

	try:
		table.set(2,2,'a')
		assert False, 'Should not set diagonal value.'
	except AssertionError:
		assert True, 'Should not set unset value.'

	# Make sure table is filling in order
	count = 1
	for row in range(tableSize):
		for col in range(row+1, tableSize):
			table.set(row, col, count)
			count += 1
	table.printTable()  #TODO: Make this into an actual test

	table = SymmetricTable_FixedSize(tableSize, includeDiagonal=True)

	try:
		table.set(2,2,'a')
		assert True, 'Should set diagonal value.'
	except AssertionError:
		assert False, 'Should be able to set value of diagonal'

	count = 1
	for row in range(tableSize):
		for col in range(row, tableSize):
			table.set(row, col, count)
			count += 1
	table.printTable() #TODO: Make this into an actual test


if __name__ == '__main__':
	testSymmetricTableFixed()
	testSymmetricTableSparse()