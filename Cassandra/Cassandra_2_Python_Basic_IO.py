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

# Verify connection with system query
from cassandra.query import SimpleStatement
query=SimpleStatement("SELECT * FROM system.local")
rs = session.execute(query)
for row in rs:
    for field in row:
        print(field)
        
# Create keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS LAB WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")

# Set keyspace
session.set_keyspace('lab')

# drop table
session.execute("drop table python_zip")

# Create table for usdata using wide rows and simple data types
create_table = \
"""
Create table python_zip ( 
 zip text, 
 state text,
 city text,
 pop int,
 longitude double,
 latitude double,
 PRIMARY KEY ( (state,city), zip)
)
"""
session.execute(create_table)

# read a few records from text file, convert JSON to python dictionary
import json
data_filename = "c:\\temp\\citydata.json"
LINES = 10
txtFile = open(data_filename,'r')
print (type(txtFile))
ix = 0
for rec in txtFile:
    if ix > LINES:
        break
    print (rec),
    drec = json.loads(rec)
    print (drec,"\n")
    ix = ix + 1
txtFile.close()

# Use a prepared statement to populate python_zip from JSON data file
from cassandra.query import PreparedStatement


prepared_stmt_text = "INSERT INTO python_zip (zip,state,city,pop,longitude,latitude) VALUES(?,?,?,?,?,?)"
prepared_stmt = session.prepare(prepared_stmt_text)

# Read in usdata json
txtFile = open(data_filename,'r')
for rec in txtFile:
    drec = json.loads(rec)
    zip = drec['_id']
    state = drec['state']
    city = drec['city']
    pop = int(drec['pop'])
    longitude = float(drec['loc'][0])
    latitude = float(drec['loc'][1])
    bound_stmt = prepared_stmt.bind([zip,state,city,pop,longitude,latitude])
    session.execute(bound_stmt)
txtFile.close()

# Count the number of records created
query = SimpleStatement("SELECT count(*) FROM python_zip")
rs = session.execute(query)
for row in rs:
    for field in row:
        print(field)
        
# Get 10 records back in JSON format
query = SimpleStatement("SELECT json zip,state,city,pop,longitude,latitude FROM python_zip")
rs = session.execute(query)
ix = 0
for row in rs:
    ix = ix + 1
    if ix > 10:
        break
    for field in row:
        print(field)



