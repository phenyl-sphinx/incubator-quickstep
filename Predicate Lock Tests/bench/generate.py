from random import randint
import sys

f = open('child.csv','w')
for i in range(1, 1000*2*int(sys.argv[1])):
	a=randint(1,10)
	b=randint(1,10)
	c=randint(1,10000)
	f.write(str(a)+'|'+str(b)+'|'+str(c)+'\n')
f.close()
f = open('parent.csv','w')
for i in range(1, 1000*2*int(sys.argv[1])):
	a=randint(1,10)
	b=randint(1,10)
	f.write(str(a)+'|'+str(b)+'\n')
f.close()
