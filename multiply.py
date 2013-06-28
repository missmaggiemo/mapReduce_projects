# Assume you have two matrices A and B in a sparse matrix format, where each
# record is of the form i, j, value.  Design a MapReduce algorithm to compute
# matrix multiplication: A x B .

# Run:

# $ python multiply.py matrix.json

import MapReduce
import sys
import json

# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
	#record: [matrix (a or b), i, j, val]
	#a: matrix w/ dim(a) = L,M
	#b: matrix w/ dim(b) = M,N	
	#for values in a, key = [i,k], value = A[i,j] (value) where k = [1:N]	
	#for values in b, key = [i,k], value = B[j,k] (value) where i = [1:L] and
	#k = [1:N]

	#dim(a) = 4,4
	#dim(b) = 4,4

	L = 5
	M = 5
	N = 5

	value = record

	if record[0] == "a":
		for k in range(N):
			key = (record[1], k)
			mr.emit_intermediate(key, value)

	elif record[0] == "b":
		for i in range(L):
			key = (i, record[2])
			mr.emit_intermediate(key, value)		

#Part 3
def reducer(key, list_of_values):
	#key: L,N
	#value: [matrix (a or b), i, j, val]
	total = []
	a = []
	b = []

	for value in list_of_values:
		if value[0] == "a":
			a.append(value)
		if value[0] == "b":
			b.append(value)

	for itema in a:
		for itemb in b:
			if itema[2] == itemb[1]:
				product = itema[3]*itemb[3]
				total.append(product)

	result = sum(total)
				
	mr.emit((key[0], key[1], result))

#Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)