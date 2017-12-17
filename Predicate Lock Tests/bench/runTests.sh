# Modify this script to generate and run the performance test cases.

# Enter the list of numbers representing the tuple count to test.  (In thousands)
tupleCountList=(20)

# Enter the list of numbers representing the predicate counts to test. (+1)
predicateCountList=(19 39 59 79 99 119 139 149 169 189 199 299 399 499 599 699 799 899 999 1099 1199 1299 1399 1499 1599 1699 1799 1899 1999)


for tuples in $tupleCountList
do
python generate.py ${tupleCountList[*]}
rm -r qsstor && ../../build/Debug/quickstep_cli_shell -initialize_db=true < create_tables.txt && ../../build/Debug/quickstep_cli_shell <importParentAndChild.txt
echo $tuples Tuples:
for number in ${predicateCountList[*]}
do
python generateMultiplePredicateTest.py $number
echo $number Predicates:
time ../../build/Debug/quickstep_cli_shell < varyingPredicateCountQueries$number.txt 2>&1 >/dev/null | grep real
done
done
