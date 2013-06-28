# Create an Inverted index. Given a set of documents, an inverted index is a
# dictionary where each word is associated with a list of the document
# identifiers in which that word appears.

# Run:

# $ python inverted_index.py books.json

import MapReduce
import sys
import json

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
	# record: list (key, value)
	# key: document id 
	# value: document contents 
	key = record[0]
	value = record[1]
	words = value.split()
	for w in words:
		mr.emit_intermediate(w, key)

#Part 3
def reducer(key, list_of_values):
	#key: word
	#value: list of doc ids
	total = []
	for v in list_of_values:
		if v not in total:
			total.append(v)
	mr.emit((key, total))

#Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)