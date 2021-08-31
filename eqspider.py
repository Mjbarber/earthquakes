import urllib.request, urllib.parse, urllib.error
import json
import sqlite3
import re
from BeautifulSoup import *
import time

# Make the DB
conn = sqlite3.connect('eqdb.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Region(
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE,
    count INTEGER
)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Zone(
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE,
    region_id INTEGER,
    count INTEGER
)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Earthquake(
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    eqid TEXT UNIQUE,
    place TEXT,
    zone_id INTEGER,
    eqtime TIME,
    eqisotime TIME,
    magnitude REAL,
    longitude REAL,
    latitude REAL,
    depth REAL,
    summary TEXT,
    rev BOOLEAN
)''')

urlservice = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson'

# 2 functions to deal with dates
def sectoiso(sec):
    neweqtupletime = time.gmtime( float(sec) )
    return time.strftime("%Y-%m-%dT%H:%M:%S", neweqtupletime)

def getdate(isodate):
    return re.findall('(.+?)T' , str(isodate))[0]

# Take the last date recorded on DB
cur.execute('SELECT max(eqtime) FROM Earthquake')
# Initial moment: 2017-1-1T00:00:00 = 1483228800 (secs) / 2000-1-1T00:00:00 = 946684800 (secs) / 2016-12-1T00:00:00 =  1480550400
initmom = 946684800
try:
    row = cur.fetchone()
    if row[0] is not None:
        lasteqtime = int( str(row[0])[:-3] )
    else:
        lasteqtime = initmom
except:
    lasteqtime = initmom

# Ask user for number of days
if lasteqtime == initmom:
    print ('There\'s no records on DB')
    plus = 0
else:
    print('The last earthquake recorded on the DB is from:', getdate( sectoiso(lasteqtime) ), lasteqtime)
    plus = 1

currtime = False
strdays = input('How many days more do you want to spider? (Enter to go until the current date):')
try:
    if strdays == '' or lasteqtime + abs(int(strdays))*86400 > time.time(): #If current date slected or date input > current date
        ndays = int( (time.time() - lasteqtime)/86400 ) + plus
        currtime = True
    else:
        ndays = abs(int(strdays)) + plus
except:
    print('You have to enter a positive integer (or press Enter for current time)')

# Make a list of 30 days max. intervals to download the earthquakes
cndays = ndays / 30
rndays = ndays % 30
dates = list()
for i in range(cndays + 1):
    dates.append( getdate (sectoiso (lasteqtime + 30*i*86400) ) )
if currtime == False:
    if rndays != 0: dates.append( getdate (sectoiso (lasteqtime + 30*(cndays)*86400 + rndays*86400) ) )
else:
    dates.append('')
print(dates)

# Arrange url and get earthquakes info and store it on DB, in 30 eqs packages
for i in range(len(dates)-1):
    starttime = dates[i]
    endtime = dates[i+1]
    print('Retrieving data, step', i+1, 'of', (len(dates)-1))
    if endtime == '':
        url = urlservice + '&' + 'starttime=' + starttime
        print('Getting earthquakes from', starttime, 'until the beginning of current date.......')
    else:
        url = urlservice + '&' + 'starttime=' + starttime + '&' + 'endtime=' + endtime
        print('Getting earthquakes from ', starttime, 'to', endtime, '........')
    print('From url:', url)

    data = urllib.request.urlopen(url).read()
    js = json.loads(data)
    #print json.dumps(js, indent = 4)

    print(js["features"][0]["properties"]["place"])

    # Count number of records before the update
    cur.execute('SELECT count(*) FROM Earthquake')
    eqnumold = cur.fetchone()[0]
    #
    eqlist = js["features"]
    count = 0
    for item in eqlist:
        count = count + 1
        eqid = item["id"]
        place = item["properties"]["place"]
        eqtime = item["properties"]["time"]
        magnitude = item["properties"]["mag"]
        longitude = item["geometry"]["coordinates"][0]
        latitude = item["geometry"]["coordinates"][1]
        depth = item["geometry"]["coordinates"][2]
        # Get a correctly formated time
        eqtimenomsec = str(eqtime)[:-3]
        eqisotime = sectoiso(eqtimenomsec)
        #
        try:
            zname = re.findall( 'of\s+(.+),', str(place) )[0]
        except:
            zname = '--'
        try:
            rname = re.findall( ',\s+(.+)', str(place) )[0]
        except:
            rname = '--'
        eqtimenomsec = str(eqtime)[:-3]
        # Summary will be retrieved and added to DB by eqdbarrange.py using BeautifulSoup

        print(place,'Zone:', zname, 'Region:', rname, magnitude, eqtime, eqisotime)

        cur.execute( 'INSERT OR IGNORE INTO Region (name) VALUES ( ? )', (rname, ) )
        cur.execute( 'SELECT count FROM Region WHERE name = ?', (rname, ) )
        countreg = cur.fetchone()[0]
        if countreg > 0:
            countreg = countreg + 1
        else:
            countreg = 1
        cur.execute( 'UPDATE Region SET count = ? WHERE name = ?', (countreg, rname ) )
        cur.execute( 'SELECT id FROM Region WHERE name = ?', (rname, ) )
        region_id = cur.fetchone()[0]

        cur.execute( 'INSERT OR IGNORE INTO Zone (name, region_id) VALUES (?, ? )', (zname , region_id) )
        cur.execute( 'SELECT count FROM Zone WHERE name = ?', (zname, ) )
        countzon = cur.fetchone()[0]
        if countzon> 0:
            countzon = countzon + 1
        else:
            countzon = 1
        cur.execute( 'UPDATE Zone SET count = ? WHERE name = ?', (countzon, zname ) )
        cur.execute( 'SELECT id FROM Zone WHERE name = ?', (zname, ) )
        zone_id = cur.fetchone()[0]

        cur.execute( '''INSERT OR IGNORE INTO Earthquake (eqid, place, zone_id, eqtime, eqisotime,  magnitude,
        longitude, latitude, depth, summary, rev) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)''',
        (eqid, place, zone_id, eqtime, eqisotime, magnitude, longitude, latitude, depth) )

    conn.commit()

# Count number of records before the update
cur.execute('SELECT count(*) FROM Earthquake')
eqnumnew = cur.fetchone()[0]
#
cur.close()

print()
print(eqnumnew - eqnumold, 'new earthquakes added to eqdb.sqlite')
