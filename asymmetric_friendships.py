# The relationship "friend" is often symmetric, meaning that if I am your
# friend, you are my friend. Implement a MapReduce algorithm to check whether
# this property holds. Generate a list of all non-symmetric friend
# relationships.

# Run:

# $ python asymmetric_friendships.py friends.json


import MapReduce
import sys
import json

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
	# record: friendship (name, name)
	names = sorted(record) 
	#makes sure that for every friend-pair, there is only one key-- the
	#alphabetically superior name
	mr.emit_intermediate(names[0], record)

#Part 3
def reducer(key, list_of_values):
	#key: person
	#value: list of friends
	for friendship in list_of_values:
		opposite = [friendship[1], friendship[0]]
		if opposite not in list_of_values:
			mr.emit((friendship[0], friendship[1]))
			mr.emit((friendship[1], friendship[0])) 
			#emits both the assymetric friendship and its opposite

#Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
