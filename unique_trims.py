# Consider a set of key-value pairs where each key is sequence id and each
# value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....

# Write a MapReduce query to remove the last 10 characters from each string of
# nucleotides, then remove any duplicates generated.

# Run:

# $ python unique_trims.py dna.json


import MapReduce
import sys
import json

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
	#record: [sequence id, nucleotides]
	#sequence id: Unique identifier formatted as a string
	#nucleotides: Sequence of nucleotides formatted as a string

	seq_id = record[0]
	nuc = record[1]
	trim = nuc[0:(len(nuc)-10)]

	key = trim[0]
	value = trim

	mr.emit_intermediate(key, value)		

#Part 3
def reducer(key, list_of_values):
	#key: first charcter of trim
	#value: trim

	unique = set(list_of_values)
	
	for item in unique:
		mr.emit((item))

#Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
