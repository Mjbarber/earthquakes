import sqlite3


connold = sqlite3.connect('eqdb.sqlite')
cur = connold.cursor()

connew = sqlite3.connect('sedb.sqlite')
cur1 = connew.cursor()
cur2 = connew.cursor()
cur3 = connew.cursor()

# Make a new DB for arrange, that it will content "Seismic events"
cur1.execute('''CREATE TABLE IF NOT EXISTS Region(
    id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    name TEXT UNIQUE,
    count INTEGER
)''')

cur1.execute('''CREATE TABLE IF NOT EXISTS Zone(
    id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    name TEXT UNIQUE,
    region_id INTEGER,
    count INTEGER
)''')

cur1.execute('''CREATE TABLE IF NOT EXISTS Earthquake(
    id INTEGER NOT NULL PRIMARY KEY UNIQUE,
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

# Copy the new earthquakes on old DB onto the new DB
eqlist = cur.execute('''SELECT id, eqid, place, zone_id, eqtime, eqisotime,
    magnitude, longitude, latitude, depth, summary FROM Earthquake WHERE rev = 0''')

for row in eqlist:
    cur1.execute('''INSERT OR IGNORE INTO Earthquake (id, eqid, place, zone_id,
    eqtime, eqisotime, magnitude, longitude, latitude, depth, summary, rev)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)''',
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]) )
connew.commit()

eqlist = cur.execute('SELECT id, name, region_id, count FROM Zone')
for row in eqlist:
    cur1.execute('''INSERT OR IGNORE INTO Zone (id, name, region_id, count)
        VALUES (?, ?, ?, 0)''', (row[0], row[1], row[2]) )
connew.commit()

eqlist = cur.execute('SELECT id, name, count FROM Region')
for row in eqlist:
    cur1.execute('''INSERT OR IGNORE INTO Region (id, name, count)
        VALUES (?, ?, 0)''', (row[0], row[1]) )
connew.commit()

# Count registers and update rev in old Earthquake DB
cur.execute('SELECT count(*) FROM Earthquake')
eqnum = cur.fetchone()[0]
cur.execute('UPDATE Earthquake SET rev = 1 WHERE rev = 0')
connold.commit()

cur.close()

# Remove aftershocks from Earthquake table on the new DB
print('Removing aftershocks and creating the new seismic events since the last previous update.......')
cont = 0
eqlist1 = cur1.execute('''SELECT eqid, zone_id, eqtime, magnitude FROM Earthquake
    WHERE rev = 0''')
for row1 in eqlist1:
    eqlist2 = cur2.execute('''SELECT eqid, zone_id, eqtime, magnitude FROM Earthquake
        WHERE rev = 0 AND abs(? -eqtime) < 86400000''', (row1[2], ) )
    for row2 in eqlist2:
        cont = cont + 1
        if cont % 100000 == 0: print('Checked', cont, 'pairs of earthquakes so far... Wait')
        if ( row1[0] != row2[0] and row1[1] == row2[1] ):
            if row1[3] < row2[3]:
                idel = row1[0]
            else:
                idel = row2[0]
            cur3.execute('DELETE FROM Earthquake WHERE eqid = ?', (idel, ) )
    cur3.execute('UPDATE Earthquake SET rev = 1 WHERE eqid = ?', (row1[0], ) )
    connew.commit()

# Create a 1 day margin where to look for matches in the next run
cur1.execute('SELECT max(eqtime) FROM Earthquake')
try:
    row = cur1.fetchone()
    if row[0] is not None:
        lasteqtime = row[0]
    else:
        lasteqtime = 0
except:
    lasteqtime = 0

cur1.execute('UPDATE Earthquake SET rev = 0 WHERE abs(? - eqtime) < 86400000', (lasteqtime, ) )
connew.commit()

# For regions, CA = California
cur1.execute('''SELECT region_id FROM Zone JOIN Region
    ON Zone.region_id = Region.id WHERE Region.name = 'California' LIMIT 1''')
californiaid = cur1.fetchone()[0]
try:
    cur1.execute('''SELECT id FROM Region WHERE name = 'CA' LIMIT 1''')
    caid = cur1.fetchone()[0]
    cur1.execute('''UPDATE Zone SET region_id = ?
        WHERE region_id = ? ''', (californiaid, caid) )
    connew.commit()
    cur1.execute('DELETE FROM Region WHERE id = ?', (caid, ) )
    connew.commit()
except:
    print()

# Count registers on new DB own write numbers on screen
cur1.execute('SELECT count(*) FROM Earthquake')
senum = cur1.fetchone()[0]

print(cont, 'pairs checked in this run ')
print(senum, 'seismic events gotten', 'from', eqnum, 'earthquakes in the whole process.')

# Count and write zones and regions on the new DB
zonedict = dict()
regiondict = dict()
eqlist1 = cur1.execute('SELECT zone_id FROM Earthquake')
for row1 in eqlist1:
    zone = row1[0]
    zonedict[zone] = zonedict.get(zone, 0) + 1
    eqlist2 = cur2.execute('SELECT region_id FROM Zone WHERE id = ?', (zone, ) )
    for row2 in eqlist2:
        region = row2[0]
        regiondict[region] = regiondict.get(region, 0) + 1

for key, value in list(zonedict.items()):
    cur1.execute('UPDATE Zone SET count = ? WHERE id = ?', (value, key ) )
connew.commit()

for key, value in list(regiondict.items()):
    cur2.execute('UPDATE Region SET count = ? WHERE id = ?', (value, key ) )
connew.commit()

print('Done, sedb.sqlite DB updated from eqdb.sqlite DB')

cur1.close()
cur2.close()
cur3.close()
