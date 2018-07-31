#install pymongo
import pip
pip.main(['install','pymongo'])

from pymongo import MongoClient
# connect
# myip = '10.1.2.31'  # localhost
# con = MongoClient(myip,27017)
con = MongoClient('localhost', 27017)

# List databases
con.database_names()

# delete test databases if they already exist
try:
    con.drop_database("pythondb")
except:
    pass
try:
    con.drop_database("pythondb2")
except:
    pass
con.database_names()

# Create two databases two different ways
db = con.pythondb
db2 = con['pythondb2']

# Lazy creation
con.database_names()

# Create two collections two different ways
my_coll = db.python_coll
my_coll2 = db2['python_coll']

# lazy creation
db.collection_names()

# Create some python variables and create a JSON document
name = 'Chloe Kim'
addr = 'Long Beach, CA'
medal = 'gold'
olympic_rec_1 =  { 'name' : name , 'address' : addr , 'medal' : medal }

# Insert the record into each collection
rec_id = my_coll.insert_one(olympic_rec_1)
rec_id2 = my_coll2.insert_one(olympic_rec_1)

# Lazy creation
con.database_names()

# Lazy creation
db.collection_names()

# Lazy creation
db2.collection_names()

# Create a second JSON document
name = 'Shawn White'
addr = 'San Diego, CA'
medal = 'gold'
olympic_rec_2 =  { 'name' : name ,  'address' : addr, 'medal' : medal }

# Insert the record into each collection
rec_id = my_coll.insert_one(olympic_rec_2)
rec_id2 = my_coll2.insert_one(olympic_rec_2)

# Print all records in the first collection
for rec in my_coll.find():
    print (rec)

# Print all records in the second collection
for rec in my_coll2.find():
    print (rec)

# Create some python variables and create a JSON document using user-defined _id
name = 'Chloe Kim'
addr = 'Long Beach, CA'
medal = 'gold'
_id = '1'
olympic_rec_1 =  { '_id' : _id , 'name' : name , 'address' : addr , 'medal' : medal }

# Insert the record into the first collection
rec_id = my_coll.insert_one(olympic_rec_1)

# Print all records in the first collection
for rec in my_coll.find():
    print (rec)

# Sample type ids
for rec in my_coll.find().limit(10):
    print (rec['_id'] , type(rec['_id']))

# Capture all different _id types
typeMap = {}
for rec in my_coll.find():
    idType = type(rec['_id'])
    if idType in typeMap:
        typeMap[idType] = typeMap[idType] + 1
    else:
        typeMap[idType] = 1
for key in typeMap:
    print (key, typeMap[key])

# Find documents with auto-generated ids
# Ref: https://docs.mongodb.com/manual/reference/operator/query/type/
for rec in my_coll.find ( { '_id' : { "$type" : "objectId" } } ):
    print (rec)

# Delete preceding
my_coll.delete_many ( { '_id' : { "$type" : "objectId" } } )

# Ensure deletion worked
for rec in my_coll.find ( { '_id' : { "$type" : "objectId" } } ):
    print (rec)
    
# Recreate a second JSON document with a user-defined id
name = 'Shawn White'
addr = 'San Diego, CA'
medal = 'silver'
_id = 2
olympic_rec_2 =  { '_id' : _id, 'name' : name , 'address' : addr , 'medal' : medal }

# Insert record
rec_id = my_coll.insert_one(olympic_rec_2)

# Print all records in the first collection
for rec in my_coll.find():
    print (rec)

# Do batch insertion into second collection after deleting it
db.collection_names()

db2.collection_names()

# Do batch insertion into second collection after deleting it
my_coll2.drop()
db2.collection_names()

db2.my_coll.insert_many([olympic_rec_1,olympic_rec_2])

# Confirm the bulk insert worked
my_coll2 = db2.my_coll.find()
for rec in my_coll2:
    print (rec)
    
# Update record
db2.my_coll.update_one ( { '_id' : 2} , { "$set" : { 'medal' : 'silver' } } , upsert=False )

# Confirm the bulk insert worked
my_coll2 = db2.my_coll.find()
for rec in my_coll2:
    print (rec)
    
# drop database db2
con.drop_database('pythondb2')

con.database_names()