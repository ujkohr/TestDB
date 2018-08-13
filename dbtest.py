# database test

import MySQLdb

conn = MySQLdb.connect(host="localhost", user="root", passwd="Dproot_", db="platypus")
cursor = conn.cursor()

#cursor.execute('update enb set idenb=5')
#cursor.execute('insert into enb (idenb) values (2)')
#conn.commit()

cursor.execute('select * from enb order by idenb desc')

#cursor.execute('select * from enb')
rowcount=cursor.rowcount
row = cursor.fetchone()

print(type(conn))
conn.close()

print(row, rowcount)
