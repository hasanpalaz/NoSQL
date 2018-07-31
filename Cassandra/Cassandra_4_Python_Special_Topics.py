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

# Map
# Reference https://docs.datastax.com/en/archived/cql/3.0/cql/cql_using/use_map_t.html
# Also set, tuple

query = "SELECT state,city,sum(pop) FROM python_zip GROUP BY state, city LIMIT 10"
rs = session.execute(query)
for row in rs:
    print (row)
    
# Create table for usdata using wide rows and simple data types
try:
    session.execute("drop table python_zip_withMap")
except:
    pass
create_table = \
"""
Create table python_zip_withMap ( 
 zip text, 
 state text,
 city text,
 pop int,
 geo map<text,double>,
 PRIMARY KEY ( state,city, zip)
)
"""
session.execute(create_table)

# Populate python_zip_byState from python_zip
from cassandra.query import PreparedStatement
from cassandra.query import SimpleStatement

query = SimpleStatement("SELECT zip, state, city, pop, longitude, latitude FROM python_zip")


prepared_stmt_text = "INSERT INTO python_zip_withMap (zip,state,city,pop,geo) VALUES(?,?,?,?,?)"

prepared_stmt = session.prepare(prepared_stmt_text)

ix = 0
rs = session.execute(query)
for row in rs:
    if ix % 1000 == 0:
        print (ix)
    ix = ix + 1
    geo = {'longitude' : row[4], 'latitude' : row[5]}
    bound_stmt = prepared_stmt.bind([row[0],row[1],row[2],row[3], geo ])
    session.execute(bound_stmt)

# query a few records
from cassandra.query import SimpleStatement

query = SimpleStatement("SELECT * from python_zip_withMap LIMIT 10")
rs = session.execute(query)
for row in rs:
    print (row)

# TTL
# Think snapchat
# How are all those deletes managed?
import time

try:
    session.execute("DROP TABLE Snap")
except:
    pass
print("\nTABLE 1 - TTL")
session.execute("""
CREATE TABLE Snap (
Sender int,
Receiver int,
PhotoId int,
Comment text,
PRIMARY KEY ( (Sender, Receiver), PhotoId )
);
""")

# Add 3 records with time-to-live quantums of 15 seconds, 15 seconds, and 30 seconds
session.execute("TRUNCATE TABLE Snap")
prepared_stmt = session.prepare ( "INSERT INTO Snap (Sender, Receiver, PhotoId, Comment) VALUES (?, ?, ?, ?) USING TTL ?")

bound_stmt = prepared_stmt.bind( [ 1, 2, 1, "One", 15])
session.execute(bound_stmt)

bound_stmt = prepared_stmt.bind( [ 1, 2, 2, "Two", 15])
session.execute(bound_stmt)

bound_stmt = prepared_stmt.bind( [ 1, 3, 1, "One", 30])
session.execute(bound_stmt)

# Using sleep, watch the records disappear as their TTLs expire
import time

print("After 0 seconds")
query=SimpleStatement("SELECT Sender, Receiver, PhotoId, ttl(Comment) FROM Snap")
rs = session.execute(query)
for row in rs:
    print (row)
    print ("")
#    for field in row:
#        print(field)
#        print("")

time.sleep(10)

print("After 10 seconds to live")
query=SimpleStatement("SELECT Sender, Receiver, PhotoId, ttl(Comment) FROM Snap")
rs = session.execute(query)
for row in rs:
    print (row)
    print ("")
#    for field in row:
#        print(field)
#        print("")

time.sleep(10)

print("After 20 seconds to live")
query=SimpleStatement("SELECT Sender, Receiver, PhotoId, ttl(Comment) FROM Snap")
rs = session.execute(query)
for row in rs:
    print (row)
    print ("")
#    for field in row:
#        print(field)
#        print("")

# Counter
try:
    session.execute("drop table counter_example")
except:
    pass
create_table = \
"""
Create table counter_example ( 
 page_url text, 
 page_name text,
 visits counter,
 PRIMARY KEY ( page_url, page_name)
)
"""
session.execute(create_table)

# No inserts, only updates
update_stmt = "UPDATE counter_example SET visits = visits + 1 WHERE page_url = 'www.yahoo.com' AND page_name = 'Yahoo Home Page'"
session.execute(update_stmt)

# query
rs = session.execute("SELECT * FROM counter_example")
for rec in rs:
    print (rec)
    
# More updates
update_stmt = "UPDATE counter_example SET visits = visits + 1 WHERE page_url = 'www.yahoo.com' AND page_name = 'Yahoo Home Page'"
session.execute(update_stmt)
update_stmt = "UPDATE counter_example SET visits = visits + 1 WHERE page_url = 'www.google.com' AND page_name = 'Google Home Page'"
session.execute(update_stmt)

# query
rs = session.execute("SELECT * FROM counter_example")
for rec in rs:
    print (rec)
    
# Update a new clustering key. What gets counted? The primary key? Or the entire key?
update_stmt = "UPDATE counter_example SET visits = visits + 1 WHERE page_url = 'www.yahoo.com' AND page_name = 'Yahoo Front Page'"
session.execute(update_stmt)

# query
# Which is preferable and when? PRIMARY KEY ( (page_url, page_name)) or PRIMARY KEY ( page_url, page_name )
rs = session.execute("SELECT * FROM counter_example")
for rec in rs:
    print (rec)
