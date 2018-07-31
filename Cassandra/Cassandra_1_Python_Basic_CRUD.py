# sudo python2 Cassandra_1_Python_Basic_CRUD.py

# Install cassandra-driver
import pip
pip.main( ['install','cassandra-driver' ] )
#pip install cassandra-driver

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

# Python Dictionary versus JSON string document

this_is_a_dictionary = { "_id" : "10002", "city" : "NEW YORK", "loc" : [ -73.98768099999999, 40.715231 ], "pop" : 84143, "state" : "NY" }

this_is_a_json_document = '{ "_id" : "10002", "city" : "NEW YORK", "loc" : [ -73.98768099999999, 40.715231 ], "pop" : 84143, "state" : "NY" }'

print (type(this_is_a_dictionary))
print (type(this_is_a_json_document))

print (this_is_a_dictionary)

print (this_is_a_json_document)

# Convert a JSON document to a Python Dictionary
import json

dict_from_json = json.loads(this_is_a_json_document)

print (type(dict_from_json))
print (dict_from_json)

# Use of a Python Dictionary
var1= int(dict_from_json['_id'])
var2= float(dict_from_json['loc'][0])
var3= dict_from_json['pop']

print (type(var1))
print (var1,"\n")

print (type(var2))
print (var2,"\n")

print (type(var3))
print (var3,"\n")

# read a few records from text file, convert JSON to python dictionary
import json

rec1 = '{ "_id" : "10002", "city" : "NEW YORK", "loc" : [ -73.98768099999999, 40.715231 ], "pop" : 84143, "state" : "NY" }'
rec2 = '{ "_id" : "02108", "city" : "BOSTON", "loc" : [ -71.068432, 42.357603 ], "pop" : 3697, "state" : "MA" }'
rec3 = '{ "_id" : "02109", "city" : "BOSTON", "loc" : [ -71.053386, 42.362963 ], "pop" : 3926, "state" : "MA" }'
rec4 = '{ "_id" : "48001", "city" : "PEARL BEACH", "loc" : [ -82.560159, 42.630704 ], "pop" : 11783, "state" : "MI" }'
rec5 = '{ "_id" : "10001", "city" : "NEW YORK", "loc" : [ -73.99670500000001, 40.74838 ], "pop" : 18913, "state" : "NY" }'
rec6 = '{ "_id" : "10002", "city" : "NEW YORK", "loc" : [ -73.98768099999999, 40.715231 ], "pop" : 84143, "state" : "NY" }'

print (type(rec1))

drec1 = json.loads(rec1)
drec2 = json.loads(rec2)
drec3 = json.loads(rec3)
drec4 = json.loads(rec4)
drec5 = json.loads(rec5)

print (drec1)
print (drec2)
print (drec3)
print (drec4)
print (drec5)

# Truncate table
session.execute("truncate python_zip")

# Use a prepared statement to populate python_zip from JSON records

from cassandra.query import PreparedStatement


prepared_stmt_text = "INSERT INTO python_zip (zip,state,city,pop,longitude,latitude) VALUES(?,?,?,?,?,?)"

prepared_stmt = session.prepare(prepared_stmt_text)

drec = drec1
zip = drec['_id']
state = drec['state']
city = drec['city']
pop = int(drec['pop'])
longitude = float(drec['loc'][0])
latitude = float(drec['loc'][1])
bound_stmt = prepared_stmt.bind([zip,state,city,pop,longitude,latitude])
session.execute(bound_stmt)


drec = drec2
zip = drec['_id']
state = drec['state']
city = drec['city']
pop = int(drec['pop'])
longitude = float(drec['loc'][0])
latitude = float(drec['loc'][1])
bound_stmt = prepared_stmt.bind([zip,state,city,pop,longitude,latitude])
session.execute(bound_stmt)

drec = drec3
zip = drec['_id']
state = drec['state']
city = drec['city']
pop = int(drec['pop'])
longitude = float(drec['loc'][0])
latitude = float(drec['loc'][1])
bound_stmt = prepared_stmt.bind([zip,state,city,pop,longitude,latitude])
session.execute(bound_stmt)

drec = drec4
zip = drec['_id']
state = drec['state']
city = drec['city']
pop = int(drec['pop'])
longitude = float(drec['loc'][0])
latitude = float(drec['loc'][1])
bound_stmt = prepared_stmt.bind([zip,state,city,pop,longitude,latitude])
session.execute(bound_stmt)

drec = drec5
zip = drec['_id']
state = drec['state']
city = drec['city']
pop = int(drec['pop'])
longitude = float(drec['loc'][0])
latitude = float(drec['loc'][1])
bound_stmt = prepared_stmt.bind([zip,state,city,pop,longitude,latitude])
session.execute(bound_stmt)

# Show all records created
query = SimpleStatement("SELECT * FROM python_zip")
rs = session.execute(query)
for row in rs:
    print (row)

# Count the number of records created
query = SimpleStatement("SELECT count(*) FROM python_zip")
rs = session.execute(query)
for row in rs:
    print (row)

# Get records with population under 4000
query = SimpleStatement("SELECT * FROM python_zip WHERE state = 'MA' AND city = 'BOSTON'")
rs = session.execute(query)
for row in rs:
    print (row)

# Update population counts for all cities
update_stmt_text = "UPDATE python_zip SET pop = ? WHERE state = 'MA' AND city = 'BOSTON' AND zip IN ('02108','02109')"

prepared_stmt = session.prepare(update_stmt_text)

val = 4000
bound_stmt = prepared_stmt.bind([val])
session.execute(bound_stmt)

query = SimpleStatement("SELECT * FROM python_zip WHERE state = 'MA' AND city = 'BOSTON'")
rs = session.execute(query)
for row in rs:
    print (row)

# delete records from Boston, MA, a single partition
delete_stmt_text = "DELETE FROM python_zip WHERE state = 'MA' and city = 'BOSTON'"
prepared_stmt = session.prepare(delete_stmt_text)
session.execute(prepared_stmt)

# delete a single "record" inside a single partition PART 1
query = SimpleStatement("SELECT * FROM python_zip")
rs = session.execute(query)
for row in rs:
    print (row)

# delete a single "record" inside a single partition PART 2
delete_stmt_text = "DELETE FROM python_zip WHERE state = 'NY' and city = 'NEW YORK' and zip = '10001'"
prepared_stmt = session.prepare(delete_stmt_text)
session.execute(prepared_stmt)

# delete a single "record" inside a single partition PART 3
query = SimpleStatement("SELECT * FROM python_zip")
rs = session.execute(query)
for row in rs:
    print (row)


