# Install cassandra-driver
import pip
pip.main( ['install','cassandra-driver' ] )

# verify 
import cassandra
print (cassandra.__version__)

# Connect
from cassandra.cluster import Cluster
cluster = Cluster ()
session = cluster.connect()

# Set keyspace
session.set_keyspace('lab')

# drop table
session.execute("drop table python_zip_byState")

# Get total population per city
query = "SELECT state,city,sum(pop) FROM python_zip GROUP BY state, city LIMIT 10"
rs = session.execute(query)
for row in rs:
    print (row)

# Create table for usdata using wide rows and simple data types
create_table = \
"""
Create table python_zip_byState ( 
 zip text, 
 state text,
 city text,
 pop int,
 longitude double,
 latitude double,
 PRIMARY KEY ( state,city, zip)
)
"""
session.execute(create_table)

# Populate python_zip_byState from python_zip
from cassandra.query import PreparedStatement
from cassandra.query import SimpleStatement

query = SimpleStatement("SELECT zip, state, city, pop, longitude, latitude FROM python_zip")


prepared_stmt_text = "INSERT INTO python_zip_byState (zip,state,city,pop,longitude,latitude) VALUES(?,?,?,?,?,?)"

prepared_stmt = session.prepare(prepared_stmt_text)

rs = session.execute(query)
for row in rs:
    bound_stmt = prepared_stmt.bind([row[0],row[1],row[2],row[3],row[4],row[5]])
    session.execute(bound_stmt)
    
# Get total population per city, sort by city
query = "SELECT state,city,sum(pop) FROM python_zip_byState WHERE state = 'AK' GROUP BY state, city ORDER BY city"
rs = session.execute(query)
for row in rs:
    print (row)

# Count zip codes per city
# Should I use python_zip or python_zip_byState?
query_python_zip = "SELECT state,city,count(*) FROM python_zip GROUP BY state,city"
rs = session.execute(query_python_zip)
for row in rs:
    print (row)

# Count zip codes per city
# Should I use python_zip or python_zip_byState?
# Reference: https://www.datastax.com/dev/blog/counting-keys-in-cassandra
query_python_zip_byState = "SELECT state,count(*) FROM python_zip_byState GROUP BY state"
rs = session.execute(query_python_zip_byState)
cnt = 0
for row in rs:
    #print row
    cnt = cnt + row[1]
print (cnt)

# Does the following count cities per state or zip codes per state?
query_python_zip_byState = "SELECT state,count(*) FROM python_zip_byState GROUP BY state"
rs = session.execute(query_python_zip_byState)
for row in rs:
    print (row)
    
# Does the following count cities per state or zip codes per state?
query_python_zip_byState = "SELECT state,count(*) FROM python_zip_byState GROUP BY state"
rs = session.execute(query_python_zip_byState)
cnt = 0
for row in rs:
    cnt = cnt + row[1]
print (cnt)

# Counting zip codes per city
query_zip_count = "SELECT state,city,zip FROM python_zip"
rs = session.execute(query_zip_count)
stateMap = {}
for row in rs:
    state = row[0]
    city = row[1]
    zip = row[2]
    if state in stateMap:
        if city in stateMap[state]:
            stateMap[state][city] = stateMap[state][city] + 1
        else:
            stateMap[state][city] = 1
    else:
        stateMap[state] = {}
            
for state in stateMap.keys():
    cityCount = stateMap[state]
    for city in cityCount:
        val = cityCount[city]
        if val > 0:
            print (state, city, val)
