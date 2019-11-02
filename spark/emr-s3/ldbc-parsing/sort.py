import sys

f = open("M", "a+")
f2 = open("N", "a+")

test=set([])
with open(sys.argv[1]+'.e') as v:
  for a in v:
    tk = a.split(' ')
    test.add(int(tk[1]))
    test.add(int(tk[0]))

o={}
cnt=0
for a in sorted(test):
        o[str(a)]=cnt
        cnt+=1

# for edge value
with open(sys.argv[1]+'.e') as v:
  for a in v:
    tk = a.split(' ')
    f.write(str(o[tk[0]])+" ")
    f.write(str(o[tk[1]])+" ")
    f.write(tk[2][:-1]+"\n")

'''
# for no edge value
with open(sys.argv[1]+'.e') as v:
  for a in v:
    tk = a.split(' ')
    f.write(str(o[tk[0]])+" ")
    f.write(str(o[tk[1][:-1]])+" ")
    f.write("1\n")
'''

with open('N_tmp') as v:
  for a in v:
    tk = a.split(' ')
    f2.write(str(o[tk[0]])+" ")
    f2.write(str(tk[1])+" ")
    f2.write("1\n")

f.close()
f2.close()
