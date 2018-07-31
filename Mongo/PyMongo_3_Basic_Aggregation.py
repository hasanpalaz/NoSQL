# connect

#myip = '10.1.2.31'  # localhost
from pymongo import MongoClient
#con = MongoClient(myip,27017)
con = MongoClient('localhost', 27017)

# List databases
con.database_names()

# open new database
db = con.pythondb

# show collections
db.collection_names()

# print 10 arbitrary documents
for doc in db.python_usdata.find().limit(10):
    print (doc)
    
# print 10 California documents
for doc in db.python_usdata.find({'state':'CA'}).limit(10):
    print (doc)
    
# Create a pipeline to sum population by State, City
my_pipeline = [ \
               { \
                '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'cityPop': {'$sum': '$pop'} \
                          } \
               } \
              ]

# Run the pipeline
cursor = db.python_usdata.aggregate(my_pipeline)
print (type(cursor))
ix = 0
for rec in cursor:
    if ix > 10:
        break
    print (rec)
    ix = ix + 1

# Create a pipeline to sum population by State, City and sort by state, city using a Python Dictionary
from bson.son import SON

my_pipeline = [ \
               { \
                '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'cityPop': {'$sum': '$pop'} \
                          } \
               } ,\
               { '$sort' : { "_id.state" : 1 , "_id.city" : 1  } }\
                ]

# Run the pipeline
cursor = db.python_usdata.aggregate(my_pipeline)
print (type(cursor))
ix = 0
for rec in cursor:
    if ix > 10:
        break
    print (rec)

# Create a pipeline to sum population by State, City and sort by city, state using a Python Dictionary
from bson.son import SON

my_pipeline = [ \
               { \
                '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'cityPop': {'$sum': '$pop'} \
                          } \
               } ,\
               { '$sort' : { "_id.city" : 1 , "_id.state" : 1  } }\
                ]
                
# Run the pipeline
cursor = db.python_usdata.aggregate(my_pipeline)
print (type(cursor))
ix = 0
for rec in cursor:
    if ix > 10:
        break
    print (rec)
    ix = ix + 1
    
# Python Dictionaries. Key-Value pairs. Keys are unordered. 
# This data structure in the mongo shell is a JSON document. They keys are ordered. In python, this is a dictionary.
x = {"a" : -1 , "b" : 1}
print (type(x))
print (x)

# Python Dictionaries. Key-Value pairs. Keys are unordered.
# This data structure in the mongo shell is a JSON document. They keys are ordered. In python this is a dictionary.
x = {"b" : -1 , "a" : 1}
print (type(x))
print (x)

# Mongo provides a utility to solve this called SON, for Serialized Ocument Notation.
# Apparent Etymology: An "O"rdered "D"ictionary Notation.
# The Dictionary of keys passed to $sort is maintained when using SON as shown below.
# What you see is what you get.
from bson.son import SON
x = SON ( [ ( "b" , -1 ) , ("a", -1 ) ] )
print (type(x))
print (x)
print (x.keys)
for key in x.keys():
    print (key)

# Mongo provides a utility to solve this called SON, for Serialized Ocument Notation.
# Apparent Etymology: An "O"rdered "D"ictionary Notation.
# The Dictionary of keys passed to $sort is maintained when using SON as shown below.
# What you see is what you get.
from bson.son import SON
x = SON ( [ ( "a" , -1 ) , ("b", -1 ) ] )
print (type(x))
print (x)
print (x.keys)
for key in x.keys():
    print (key)
    
# Create a pipeline to sum population by State, City and sort by State, City
from bson.son import SON

my_pipeline = [ \
               { \
                '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'cityPop': {'$sum': '$pop'} \
                          } \
               } ,\
               { '$sort' : SON ( [ ( "_id.state" , 1 ) , ("_id.city", 1 ) ] ) }
              ]

# Run the pipeline
cursor = db.python_usdata.aggregate(my_pipeline)
print (type(cursor))
ix = 0
for rec in cursor:
    if ix > 10:
        break
    print (rec)
    ix = ix + 1

# Create a pipeline to sum population by State, City and sort by city, state
from bson.son import SON

my_pipeline = [ \
               { \
                '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'cityPop': {'$sum': '$pop'} \
                          } \
               } ,\
               { '$sort' : SON ( [ ( "_id.city" , 1 ) , ("_id.state", 1 ) ] ) }
              ]

# Run the pipeline
cursor = db.python_usdata.aggregate(my_pipeline)
print (type(cursor))
ix = 0
for rec in cursor:
    if ix > 10:
        break
    print (rec)
    ix = ix + 1

# executionStats are not available in PyMongo
db.command("aggregate","python_usdata",pipeline=my_pipeline,explain=True)

# MATCH
# Create a pipeline to filter on CA, sum population by State, City, and sort by cityPop
from bson.son import SON

my_pipeline = [ \
               {\
                '$match' : {\
                            "state" : "CA"\
                           } \
               },\
               {
               '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'cityPop': {'$sum': '$pop'} \
                          } \
               } ,\
               { '$sort' : SON ( [ ( "cityPop" , -1 ) , ("_id.city", 1 ) ] ) }
              ]
              
# Run the pipeline
cursor = db.python_usdata.aggregate(my_pipeline)
print (type(cursor))
ix = 0
for rec in cursor:
    if ix > 10:
        break
    print (rec)
    ix = ix + 1

# UNWIND
# Normalizes vectors
# Reference: https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/
rec = { "_id" : 1, "item" : "ABC1", "sizes" : [ "S", "M", "L"] }
db.unwind_example.insert_one(rec)

# UNWIND
# raw data
cursor = db.unwind_example.find()
for rec in cursor:
    print (rec)

# UNWIND
# unwound data
my_pipeline = [ { "$unwind" : "$sizes"} ]
cursor = db.unwind_example.aggregate( my_pipeline )
for rec in cursor:
    print (rec)

# project
my_pipeline = [ { "$project" : { "city":1, "state":1, "pop" : 1 } } ]
cursor = db.python_usdata.aggregate( my_pipeline )
ix = 0
for rec in cursor:
    print (rec)
    ix = ix + 1
    if ix > 10:
        break

# Count Zip Codes in each state, sort the states
my_pipeline = [ \
               {\
               '$group': { \
                           '_id': { "state": "$state" } ,\
                           'zipCount': {'$sum': 1} \
                          } \
               } ,\
               { '$sort' : SON ( [ ("_id.state", 1 ) ] ) }\
             ]
             
# run the pipeline
cursor = db.python_usdata.aggregate( my_pipeline )
for rec in cursor:
    print (rec)

# Count the zip codes in each city in AK. Sort by count.
my_pipeline = [ \
               {\
                '$match' : {\
                            "state" : "AK"\
                           } \
               },\
               {
               '$group': { \
                           '_id': { "state": "$state","city": "$city" } ,\
                           'zipCount': {'$sum': 1} \
                          } \
               } ,\
               { '$sort' : SON ( [ ( "zipCount" , -1 ) ] ) }
              ]
# run the pipeline
cursor = db.python_usdata.aggregate( my_pipeline )
for rec in cursor:
    print (rec)

    