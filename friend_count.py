# Consider a simple social network dataset consisting of key-value pairs where
# each key is a person and each value is a friend of that person. Describe a
# MapReduce algorithm to count he number of friends each person has.

# Run:

# $ python friend_count.py friends.json


import MapReduce
import sys
import json

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
	# key: person
	# value: friend
	key = record[0]
	value = record[1]
	mr.emit_intermediate(key, value)

#Part 3
def reducer(key, list_of_values):
	#key: person
	#value: list of friends
	total = len(list_of_values)
	mr.emit((key, total))

#Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)