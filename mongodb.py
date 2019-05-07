import pymongo
import datetime
from pymongo import MongoClient
import time



client = MongoClient('localhost', 27017)
db = client.comp90050


##
def CreateCollection():
    # initial a collection (micro seconds) 2.8
    starttime = int(round(time.time() * 1000000))
    users = db["users"]
    endtime=int(round(time.time() * 1000000))
    return (endtime-starttime)

def testInitial():
    average = 0
    for i in range(10):
        average += CreateCollection()
        db.users.drop()
    print(average/10)

# 2.8 microsec

# prepare test data
def generateTestData():
    # key, value
    users = db["users"]
    for i in range(20000):
        user = { "key": i, "value": i }
        users.insert_one(user)


##read
def read(times):
    users = db["users"]
    for i in range(times):
       x=users.find_one({"key": i})
       #x = users.find_one()
       #print(x)

def readtime(times):
    starttime = int(round(time.time() * 1000))
    for i in range(5):
        read(times)
    endtime=int(round(time.time() * 1000))
    average = (endtime - starttime)/5
    print("Read "+ str(times) +" times: " + str(average) + " ms")

def testRead():
    db.users.drop()
    generateTestData()
    #test 10, 50, 100, 1000, 10000, 20000
    test = [10, 50, 100, 1000, 10000, 20000]
    for num in test:
        readtime(num)

#testRead()
#Read 10 times: 4.0 ms
#Read 50 times: 22.4 ms
#Read 100 times: 44.4 ms
#Read 1000 times: 653.0 ms
#Read 10000 times: 27876.4 ms
#Read 20000 times: 116256.2 ms


##write
def write(times):
    users = db["users"]
    for i in range(times):
        user = { "key": i, "value": i }
        users.insert_one(user)

def writetime(times):
    starttime = int(round(time.time() * 1000))
    for i in range(5):
        write(times)
    endtime=int(round(time.time() * 1000))
    average = (endtime - starttime)/5
    print("Write "+ str(times) +" times: " + str(average) + " ms")

def testWrite():
    db.users.drop()
    test = [10, 50, 100, 1000, 10000, 20000]
    for num in test:
        writetime(num)

# testWrite()
# Write 10 times: 14.2 ms
# Write 50 times: 22.0 ms
# Write 100 times: 46.8 ms
# Write 1000 times: 382.6 ms
# Write 10000 times: 4277.6 ms
# Write 20000 times: 8099.4 ms

##delete
def delete(times):
    db.users.drop()
    generateTestData()
    starttime = int(round(time.time() * 1000))
    users = db["users"]
    for i in range(times):
        user = {"key": i}
        users.delete_one(user)
    endtime=int(round(time.time() * 1000))
    return (endtime - starttime)

def deletetime(times):
    total = 0
    for i in range(5):
        total += delete(times)
    average =  total/5
    print("Delete "+ str(times) +" times: " + str(average) + " ms")

def testDelete():

    test = [10000, 20000]
    for num in test:
        deletetime(num)

# testDelete()

# Delete 10 times: 5.4 ms
# Delete 50 times: 20.6 ms
# Delete 100 times: 43.6 ms
# Delete 1000 times: 476.2 ms
# Delete 10000 times: 4620.6 ms
# Delete 20000 times: 9488.6 ms


##get all keys

def getAll():
    users = db["users"]
    users.find()

def testgetAll():
    db.users.drop()
    generateTestData()
    starttime = int(round(time.time() * 1000000))
    for i in range(5):
        getAll()
    endtime=int(round(time.time() * 1000000))
    average = (endtime - starttime) / 5
    print("GetAll: " + str(average) + " micros")


# testgetAll()

# 28.6 microseconds







