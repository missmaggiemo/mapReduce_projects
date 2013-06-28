# Implement a relational join as a MapReduce query.

# Consider the query:

# SELECT * 

# FROM Orders, LineItem 

# WHERE Order.order_id = LineItem.order_id

# Your MapReduce query should produce the same information as this SQL query.
# You can consider the two input tables, Order and LineItem, as one big
# concatenated bag of records which gets fed into the map function record by
# record.

# Run:

# $ python join.py records.json

import MapReduce
import sys
import json

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
	# record: list (tag, key, values)
	# tag: table of origin (order or line_item
	# key: order_id
	# value: data of indeterminate length
	key = record[1] 
	value = record[0:len(record)]
	mr.emit_intermediate(key, value)


#Part 3
def reducer(key, list_of_values):
	# tag: table of origin (order or line_item)
	# key: order_id
	# value: data of indeterminate length
	total = {}
	table1 = [] #order
	table2 = [] #line_item
	
	for value in list_of_values:
		if value[0] == "order":
			table1.append(value)
		if value[0] == "line_item":
			table2.append(value)

	for item1 in table1:
		for item2 in table2:
			item3 = list(item1) #copy item1
			for i in item2:
				item3.append(i)
			mr.emit((item3))

#Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
