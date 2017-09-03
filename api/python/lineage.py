from quickstep import Quickstep

qs = Quickstep('/Users/jianqiao/Desktop/incubator-quickstep/build/Debug/quickstep_client')

vertices = [1299454, 31623412, 31739916, 31593563]

# Truncate old tables.
qs.execute('DROP TABLE cur; DROP TABLE next; DROP TABLE answer;')
qs.execute('CREATE TABLE cur(id INT); CREATE TABLE next(id INT); CREATE TABLE answer(id INT);')

stmts = ''
for v in vertices:
  stmts += 'INSERT INTO cur VALUES(' + str(v) + '); '
qs.execute(stmts)
qs.execute('INSERT INTO answer SELECT id FROM cur;')

