from random import randint

numberOfPredicates= [1,10,100,1000,10000]
numberOfQueries = 100

for preds in numberOfPredicates:
	f = open('varyingPredicateCountQueries'+str(preds)+'.txt','w')
	query='UPDATE Child SET a=1'
	if preds>1:
		query=query+' WHERE '
		for i in range(1, preds):
			query=query+'a='+str(i)+' '
			if not i==preds-1:
				query=query+'OR '
	for i in range(1,numberOfQueries):
		f.write(query+';\n')
	f.write('\dt')
	f.close()
