from random import randint

numberOfPredicates= [1,10,100,1000,10000]
numberOfQueries = 10
setCounter=1;
whereCounter=1000000;

for preds in numberOfPredicates:
	f = open('varyingPredicateCountQueries'+str(preds)+'.txt','w')
	for i in range(1,numberOfQueries):
		query='UPDATE Child SET a='+str(setCounter)
		setCounter=setCounter+1;
		wherequery=''
		if preds>0:
			wherequery=wherequery+' WHERE '
			for i in range(0, preds):
				wherequery=wherequery+'a='+str(whereCounter)+' '
				whereCounter=whereCounter+1
				if not i==preds-1:
					wherequery=wherequery+'OR '
		query=query+wherequery
		f.write(query+';\n')
	f.write('\dt')
	f.write('\n')
	f.write('\n')
	f.close()
