# This program sets the rev fiel of all the earthquakes to 0,
# on the old DB in order that the table can be checked again by eqclean.py

import sqlite3

conn = sqlite3.connect('eqdb.sqlite')
cur = conn.cursor()

cur.execute('UPDATE Earthquake SET rev = 0')
conn.commit()

print('Done, the rev fiel of all the earthquakes has been set to 0 on sqdb.py')
