# make random '<M's col> by 100' matrix

import sys
import random

testsite_array=[]
with open(sys.argv[1]+".v") as vertices:
  for v in vertices:
    testsite_array.append(v[:-1])

start_points=random.sample(range(len(testsite_array)), 100)
positions=random.sample(range(100), 100)
start_points.sort()

with open('N_tmp', 'w') as new:
  for i in range (0, len(start_points)): # row
    value=str(testsite_array[start_points[i]])+" "+str(positions[i])+" "+"1.0"+"\n"
    new.write(value)

