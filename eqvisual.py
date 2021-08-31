import sqlite3
import matplotlib.pyplot as plt
import mpld3
from mpl_toolkits.basemap import Basemap
import seaborn
import re
from bokeh.plotting import figure, output_file, show
import string
from wordcloud import WordCloud

conn = sqlite3.connect('sedb.sqlite')
cur = conn.cursor()

def histogr(nregions):
    reg= dict()
    eqlist = cur.execute('''SELECT name, count FROM Region WHERE name != "--"
        ORDER BY count DESC LIMIT ?''', (nregions, ) )
    for row in eqlist:
        print(row)
        rname = row[0]
        rcount = row[1]
        reg[rname] = rcount
    sortedlist = sorted ( [ (v, k) for k, v in list(reg.items()) ], reverse = True)
    print(sortedlist)
    barslist = list()
    xtickslist = list()
    colors = ['red', 'blue', 'green', 'orange', 'yellow', 'black']
    colorslist = list()
    for i in range(len(sortedlist) ):
        barslist.append(sortedlist[i][0])
        xtickslist.append(sortedlist[i][1])
        colorslist.append(colors[i % len(colors)])
    print(sortedlist)

    plt.figure(figsize=(15,6))
    plt.bar(list(range(len(reg))), barslist, align='center', color = colorslist)
    plt.xticks(list(range(len(reg))), xtickslist, size = 12)
    plt.show()

def wmap(minmag):
    eqid = list()
    eqplace = list()
    eqlong = list()
    eqlat = list()
    eqmag = list()
    eqlist = cur.execute('''SELECT eqid, place, longitude, latitude, magnitude
        FROM Earthquake WHERE magnitude >= ? AND instr(eqisotime, 2017) > 0''', (minmag, ) )
    for row in eqlist:
        if row[3] >= minmag:
            eqid.append(row[0])
            eqplace.append(row[1])
            eqlong.append(row[2])
            eqlat.append(row[3])
            eqmag.append(row[4])
    # Set size of the Basemap
    plt.figure(figsize=(30,12))
    # Create a map on which to draw.  We're using a mercator projection, and showing the whole world.
    # m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
    m = Basemap(projection='robin',lon_0=0,resolution='c')
    # Draw coastlines, and the edges of the map.
    m.drawcoastlines()
    m.drawcountries(linewidth=0.25)
    m.drawmapboundary(fill_color = '#46bcec')
    m.fillcontinents(color='Khaki',lake_color='#46bcec')
    # Use matplotlib to draw the points onto the map.
    for i in range(0, len(eqmag) ):
        # Convert latitude and longitude to x and y coordinates
        x, y = m(eqlong[i], eqlat[i])
        # label is a list with only one element
        label= list()
        st = str(eqplace[i]) + '      Mag:' + str(eqmag[i])
        label.append(st)
        print(eqid[i])
        try:
            size = 3*eqmag[i]
        except:
            continue
        mapa = m.plot(x, y, 'o', markersize = size, color = 'red', alpha = 0.8)
        tooltip = mpld3.plugins.PointLabelTooltip(mapa[0], labels = label)
        mpld3.plugins.connect(plt.gcf(), tooltip)
        plt.title('Seismic activity in 2017', size = 25)
    # Show the plot.
    mpld3.show()


    # Create a map on which to draw.  We're using a mercator projection, and showing the whole world.
    # m = Basemap(projection='robin',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')
    # fig, ax = Basemap(projection = 'robin', resolution = 'l', area_thresh = 1000.0, lat_0 = 0, lon_0 = -130)
    # Draw coastlines, and the edges of the map.
    # ax.drawcoastlines()
    # ax.fillcontinents(color='#f2f2f2',lake_color='#46bcec')
    # ax.drawmapboundary()
    # Use matplotlib to draw the points onto the map.

    # mapa = ax.plot(eqlong, eqlat, 'o', markersize = 2, color = 'red', alpha = 0.8)
    # tooltip = mpld3.plugins.PointLabelTooltip(fig, labels= eqid)
    # mpld3.plugins.connect(fig, tooltip)
    # Show the plot.
    # mpld3.show()


def lines(nregions):
    reglist= list()
    regdict = dict()
    # cur.execute('SELECT eqisotime FROM Earthquake ORDER BY eqtime ASC LIMIT 1')
    # minyear = int( str(cur.fetchone()[0])[0:4] )
    minyear = 2013
    cur.execute('SELECT eqisotime FROM Earthquake ORDER BY eqtime DESC LIMIT 1')
    maxyear = int( str(cur.fetchone()[0])[0:4] )
    regionlist = cur.execute('''SELECT name FROM Region WHERE name != "--"
        ORDER BY count DESC LIMIT ?''', (nregions, ) )
    for row in regionlist:
        reglist.append(str(row[0]))
    for region in reglist:
        for year in range(minyear, maxyear):
            eqlist = cur.execute('''SELECT sum(Earthquake.magnitude) FROM Earthquake JOIN Zone JOIN Region
            ON Earthquake.zone_id = Zone.id AND Zone.region_id = Region.id
            WHERE Region.name = ? AND instr(Earthquake.eqisotime, ?) > 0''', (region, str(year)) )
            regdict[region, year] = cur.fetchone()[0]
    # prepare some data
    x =  list(range(minyear, maxyear))
    print()
    y = list()
    for i in range(nregions):
        y.append(list())
        for year in range(minyear, maxyear):
            y[i].append(regdict[reglist[i], year])
        print(y[i])

    # y1 = [10**i for i in x]
    # y2 = [10**(i**2) for i in x]

    # output to static HTML file
    output_file("lines.html")

    # create a new plot
    p = figure(title="Seismic events", x_axis_label='Year', y_axis_label='Acumulated magnitude',
        plot_width=1000, plot_height=600 #, y_range=[0, 2000]
        )

    # add some renderers
    colors = ['red', 'blue', 'green', 'orange', 'yellow', 'black']
    p.line(x, x, legend="y=" + reglist[0])
    # p.circle(x, x, legend="y=x", fill_color="white", size=8)
    for i in range(nregions):
        p.line(x, y[i], legend="y=" + reglist[i], line_width=2, line_color=colors[i % len(colors)] )

    # show the results
    show(p)

def wcloud():
    manf = open('summaries.txt', 'w')
    summlist = cur.execute('''SELECT summary FROM Earthquake
        WHERE summary IS NOT NULL AND summary != 'No Tectonic Summary' ''')
    for row in summlist:
        text = str(row[0])
        text = text.translate(None, string.punctuation)
        text = text.translate(None, '1234567890')
        text = text.strip()
        text = text.lower()
        words = text.split()
        newsumm = ''
        for word in words:
            if len(word) > 3: newsumm = newsumm + word + ' '
        manf.write(newsumm)
    manf.close()

    text = open('summaries.txt').read()

    # Generate a word cloud image
    wordcloud = WordCloud().generate(text)

    # Display the generated image:
    # the matplotlib way:
    plt.imshow(wordcloud)
    plt.axis("off")

    # lower max_font_size
    wordcloud = WordCloud(max_font_size=40).generate(text)
    plt.figure()
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.show()

# The pil way (if you don't have matplotlib)
#image = wordcloud.to_image()
#image.show()


# histogr(10)
# wmap(1)
# lines(5)
wcloud()
