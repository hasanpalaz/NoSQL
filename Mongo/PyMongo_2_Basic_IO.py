# connect
#myip = '10.1.2.31'  # localhost
from pymongo import MongoClient
#con = MongoClient(myip,27017)
con = MongoClient('localhost', 27017)

# List databases
con.database_names()

# open usdata
db = con.usdata

# show collections
db.collection_names()

# print 10 arbitrary documents
for doc in db.cityinfo.find().limit(10):
    print (doc)
    
# print 10 California documents
for doc in db.cityinfo.find({'state':'CA'}).limit(10):
    print (doc)
    
# Convert bson to json
from bson import json_util
dataFile = open("citydata.json",'w')  # overwrites file
for doc in db.cityinfo.find({'state':'CA'}).limit(10):
    print (json_util.dumps(doc))

# Write all records to a text file
data_filename = "c:\\temp\\citydata.json"
dataFile = open(data_filename,'w')  # overwrites file
for doc in db.cityinfo.find({}):
    dataFile.write(json_util.dumps(doc))
    dataFile.write("\n")
dataFile.close()

# Count all lines in file
num_lines = sum(1 for line in open(data_filename))
print (num_lines)

# read  LINES records from text file
LINES = 10
txtFile = open(data_filename,'r')
print (type(txtFile))
ix = 0
for rec in txtFile:
    if ix > LINES:
        break
    print (rec),
    ix = ix + 1
txtFile.close()

# Check databases
con.database_names()

# Create a new db
try:
    con.drop_database("pythondb")
except:
    pass
db2 = con["pythondb"]

# Lazy Creation
con.database_names()

# Lazy creation
db2.collection_names()

# Add documents from text file to new db
# Convert JSON to python object so PyMongo can convert the Python object to BSON
txtFile = open("c:\\temp\\citydata.json",'r')
for rec in txtFile:
    obj = json_util.loads(rec)
    db2.python_usdata.insert_one(obj)
txtFile.close()

con.database_names()

db2.collection_names()

# check the data just created
for doc in db2.python_usdata.find({}).limit(10):
    print (doc)
    

