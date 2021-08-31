import urllib.request, urllib.parse, urllib.error
import sqlite3
import re
from BeautifulSoup import *
import string

conn = sqlite3.connect('eqdb.sqlite')
cur = conn.cursor()
cur2 = conn.cursor()

# Function to get a rough Summary with BeautifulSoup
def getsummary(ideq):
    bsurl = 'https://earthquake.usgs.gov/earthquakes/eventpage/' + ideq + '#executive'
    print(bsurl, end=' ')
    html = urllib.request.urlopen(bsurl).read()
    soup = BeautifulSoup(html)
    labels = soup('script')
    summa= '-No Tectonic Summary--'
    for label in labels:
        if 'Tectonic Summary' in str(label):
            summa = re.findall( 'Tectonic Summary(.+?)}}}', str(label) )[0]
    return summa

# Function to clean the rough Summary
def cleansummary(str):
    inreplace = ['<', '>', '/h2','\/h2', '\n', '/p', '\\np','\\n', '\p']
    for item in inreplace:
        str = str.replace(item, '')
    str = str.replace('\\u2019', '\'')
    str = str.replace('\/', '/')
    str = str.replace('\\u00a0', ' ')
    str = str.replace('\\u2013', '-')
    if len(str) > 2:
        return str[1: -2]
    else:
        return ''

# Ask minimum magnitud
try:
    mmag= input('Minimum magnitude for summary (1-12)? (5 by default):')
    if len(mmag) < 1: mmag = 5
    minmag = float(mmag)
except:
    print('Insert a number between 0 and 12')
    quit()

# Write summaries on DB
eqlist = cur.execute('''SELECT eqid FROM Earthquake
    WHERE summary is NULL AND magnitude >= ?''', (minmag, ) )
for row in eqlist:
    summary = getsummary(row[0])
    summary = cleansummary(summary)
    print('-->', summary)
    cur2.execute('UPDATE Earthquake SET summary = ? WHERE eqid = ?', (summary, row[0]) )
    conn.commit()

cur.close()
cur2.close()

print('Done')
