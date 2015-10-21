import httplib
import urllib
import urllib2 
import re
import csv
import logging
import psycopg2
import time
import pandas.io.sql as psql
import pandas as pd
import tinys3
import datetime
from cookielib import CookieJar

class pyGTrends( object ):
    """
    Google Trends API
    HOW TO INTERPRET GTRENDS VALUES:
    Numbers represent search interest relative to the highest point on the chart.
    If at most 10% of searches for the given region and time frame were for "pizza," we'd consider this 100.
    This doesn't convey absolute search volume. The numbers on the graph reflect how many searches have been done for a particular term,
    relative to the total number of searches done on Google over time. They don't represent absolute search volume numbers,
    because the data is normalized and presented on a scale from 0-100.
    Each point on the graph is divided by the highest point and multiplied by 100.
    When we don't have enough data, 0 is shown.
    """
    def __init__( self, username, password ):
        """
        provide login and password to be used to connect to Google Analytics
        all immutable system variables are also defined here
        website_id is the ID of the specific site on google analytics
        """        
        self.login_params = {
            "continue": 'http://www.google.com/trends',
            "PersistentCookie": "yes",
            "Email": username,
            "Passwd": password,
        }
        self.headers = [ ( "Referrer", "https://www.google.com/accounts/ServiceLoginBoxAuth" ),
                         ( "Content-type", "application/x-www-form-urlencoded" ),
                         ( 'User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21' ),
                         ( "Accept", "text/plain" ) ]
        self.url_ServiceLoginBoxAuth = 'https://accounts.google.com/ServiceLoginBoxAuth'
        self.url_Export = 'http://www.google.com/trends/viz'
        self.url_CookieCheck = 'https://www.google.com/accounts/CheckCookie?chtml=LoginDoneHtml'
        self.url_PrefCookie = 'http://www.google.com'
        self.header_dictionary = {}
        self._connect()
        
    def _connect( self ):
        """
        connect to Google Trends
        """
        
        self.cj = CookieJar()                            
        self.opener = urllib2.build_opener( urllib2.HTTPCookieProcessor( self.cj ) )
        self.opener.addheaders = self.headers
        galx = re.compile( '<input name="GALX"[\s]+type="hidden"[\s]+value="(?P<galx>[a-zA-Z0-9_-]+)">' )
        resp = self.opener.open( self.url_ServiceLoginBoxAuth ).read()
        resp = re.sub( r'\s\s+', ' ', resp )
        #print resp
        m = galx.search( resp )
        if not m:
            raise Exception( "Cannot parse GALX out of login page" )
        self.login_params['GALX'] = m.group( 'galx' )
        params = urllib.urlencode( self.login_params )
        self.opener.open( self.url_ServiceLoginBoxAuth, params )
        self.opener.open( self.url_CookieCheck )
        self.opener.open( self.url_PrefCookie )
        
    def download_report( self, keywords, date='all', geo='all', geor='all', graph = 'all_csv', sort=0, scale=0, sa='N' ):
        """
        download a specific report
        date, geo, geor, graph, sort, scale and sa
        are all Google Trends specific ways to slice the data
        """
        if type( keywords ) not in ( type( [] ), type( ( 'tuple', ) ) ):
            keywords = [ keywords ]
        
        params = urllib.urlencode({
            'q': ",".join( keywords ),
            'date': date,
            'graph': graph,
            'geo': geo,
            'geor': geor,
            'sort': str( sort ),
            'scale': str( scale ),
            'sa': sa
        })                            
        self.raw_data = self.opener.open( 'http://www.google.com/trends/viz?' + params ).read()
        if self.raw_data in ['You must be signed in to export data from Google Trends']:
            logging.error('You must be signed in to export data from Google Trends')
            raise Exception(self.raw_data)
        
    def csv(self, section="Main", as_list=False):
        """
        Returns a CSV of a specific segment of the data.
        Available segments include Main, City and Subregion.
        """
        if section == "Main":
            section = ("Week","Year","Day","Month")
        else:
            section = (section,)
            
        segments = self.raw_data.split('\n\n\n')
        start = []
        found = False
        for i in range( len( segments ) ):
            lines = segments[i].split('\n')
            n = len(lines)
            for counter, line in enumerate( lines ):
                if line.partition(',')[0] in section or found:
                    if counter + 1  != n: # stops us appending a stupid blank newspace at the end of the file
                        start.append( line + '\n' )
                    else :
                        start.append( line )
                    found = True
            segments[i] = ''.join(start)
            
        for s in segments:
            if s.partition(',')[0] in section:
                if as_list:
                    return [line for line in csv.reader(s.split('\n'))]
                else:
                    return s
        logging.error("Could not find requested section")
        raise Exception("Could not find requested section")

"""
read the list of keywords from a table in redshift
"""
db = psycopg2.connect(host="host-name",database="db",port="***",user="replace_me",password="replace_me")
sqlq = "select keyword from gtrends_keywords_to_pull where whitelist=1;"
resultdata = psql.read_sql(sqlq, db)
db.close()


username = 'replace-me'#raw_input('Enter your Google username: \n')
password = 'replace-me'#raw_input('Enter your Google password: \n')
a = pyGTrends(username, password)
geo='US'

now = datetime.datetime.now()
year=now.year
month=now.month

if month<10:
    date='%d-0%d'%(year,month)
else:
    date='%d-%d'%(year,month)
#time.sleep(4) 
dfList = resultdata.keyword.tolist()
DS={}
To_delete=[]
No_day=[]

for item in dfList:
    time.sleep(3)
    keywords = item
    a.download_report(keywords,date,geo)
    try:
        data = a.csv( section='Main' ).split('\n')
        clean_data=[]
        for line in data:
            clean_data.append(line.split(','))
        if clean_data[0][0]=='Day':
            DataSet = pd.DataFrame(clean_data[1:len(clean_data)],columns=clean_data[0])
            DataSet=DataSet.rename(columns = {item:'GTrend_Value'})
            DataSet['Keyword'] = [item for i in xrange(0,len(clean_data)-1)]
            DataSet['Region']=[geo for i in xrange(0,len(clean_data)-1)]
            DS['df_%s' % item]=DataSet
        else:
            No_day.append(item)
                
    except Exception, e:
        print 'Error:',e
        print 'item:',item
        To_delete.append(item)
        continue
for x in To_delete:
    dfList.remove(x)
for y in No_day:
    dfList.remove(y)
    
"""uploading result on s3"""

if len(dfList)>0:
    GTrend_data=pd.concat([DS['df_%s' % item] for item in dfList])
    GTrend_data.to_csv('~\\GTrend-data\\Gtrends-%s-%s.csv' % (date,geo),index=False)
    conn = tinys3.Connection('replace-me','replace-me',endpoint='s3-us-west-2.amazonaws.com')
    f = open('~\\GTrend-data\\Gtrends-%s-%s.csv' % (date,geo),'rb')
    conn.upload('Gtrends-%s-%s.csv' % (date,geo),f,'replace-me')


