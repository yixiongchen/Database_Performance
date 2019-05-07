import psycopg2
import time
mydb = psycopg2.connect(user = "yixiongchen",
                                  password = "cyx19941026",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "comp90050")
cursor = mydb.cursor()





## Create Table
def createTable():
    # Drop table if it already exist using execute() method.
    # cursor.execute("DROP TABLE IF EXISTS USERS")
    starttime = int(round(time.time() * 1000000))
    sql = """CREATE TABLE IF NOT EXISTS USERS (ID INT,VALUE INT)"""
    cursor.execute(sql)
    endtime=int(round(time.time() * 1000000))
    return (endtime-starttime)

def testTable():
    average = 0
    for i in range(5):
        average += createTable()
        cursor.execute("DROP TABLE IF EXISTS USERS")
    print(average/5)
    mydb.close()

# testTable()
# 10603 micro seconds

## prepare data set
def generateDataSet():
   sqltable = """CREATE TABLE IF NOT EXISTS USERS (ID INT,VALUE INT)"""
   cursor.execute(sqltable)
   for i in range(20000):
      insertsql = """INSERT INTO USERS VALUES (%s, %s)"""
      cursor.execute(insertsql, (i, i))
      mydb.commit()

   #cursor.close()
   #mydb.close()

#generateDataSet()



## Read
def read(times):
   sql = """SELECT * FROM USERS WHERE ID = %s"""
   for i in range(times):
      cursor.execute(sql, (i,))

def readTime(times):
   starttime = int(round(time.time() * 1000))
   for i in range(5):
      read(times)
   endtime=int(round(time.time() * 1000))
   average = (endtime - starttime)/5
   print("Read "+ str(times) +" times: " + str(average) + " ms")

def testRead():
   test = [10, 50, 100, 1000, 10000, 20000]
   for num in test:
      readTime(num)
   cursor.close()
   mydb.close()

# testRead()
# Read 10 times: 14 ms
# Read 50 times: 71 ms
# Read 100 times: 143 ms
# Read 1000 times: 1469 ms
# Read 10000 times: 14437 ms
# Read 20000 times: 29108 ms

## Write
# Prepare SQL query to INSERT a record into the database.
def write(times):
   insertsql = """INSERT INTO USERS VALUES (%s, %s)"""
   for i in range(times):
      cursor.execute(insertsql, (i, i))
      mydb.commit()

def writeTime(times):
   cursor.execute("CREATE TABLE IF NOT EXISTS USERS (ID INT,VALUE INT)")
   print("Table created")
   starttime = int(round(time.time() * 1000))
   for i in range(5):
      write(times)
   endtime=int(round(time.time() * 1000))
   average = (endtime - starttime)/5
   print("Write "+ str(times) +" times: " + str(average)+" ms")
   cursor.execute("DROP TABLE IF EXISTS USERS")
   print("Table dropped")

def testWrite():
    test = [10, 50, 100, 1000, 10000, 20000]
    for num in test:
        writeTime(num)

# testWrite()

# Write 10 times: 12 ms
# Write 50 times: 19 ms
# Write 100 times: 35 ms
# Write 1000 times: 322 ms
# Write 10000 times: 3386 ms
# Write 20000 times: 6594 ms



## Delete
def delete(times):
   generateDataSet()
   starttime = int(round(time.time() * 1000))
   sql = "DELETE FROM USERS WHERE ID = '%s' and VALUE = '%s'"
   for i in range(times):
      cursor.execute(sql, (i, i))
      mydb.commit()
   endtime=int(round(time.time() * 1000))
   cursor.execute("DROP TABLE IF EXISTS USERS")

   return (endtime - starttime)

def deletetime(times):
    total = 0
    for i in range(5):
        total += delete(times)
    average =  total/5
    print("Delete "+ str(times) +" times: " + str(average) + " ms")

def testDelete():

    test = [10, 50, 100, 1000, 10000, 20000]
    for num in test:
        deletetime(num)


# testDelete()
# Delete 10 times: 20 ms
# Delete 50 times: 97 ms
# Delete 100 times: 191 ms
# Delete 1000 times: 1885 ms
# Delete 10000 times: 15743 ms
# Delete 20000 times: 25215 ms


## GetALL

def getAll():
   sql = "SELECT * FROM USERS"
   cursor.execute(sql)


def testgetAll():
   #generateDataSet()
   starttime = int(round(time.time() * 1000000))
   for i in range(5):
      getAll()
   endtime=int(round(time.time() * 1000000))
   average = (endtime - starttime) / 5
   print(average)

# testgetAll()
# 5577 microseconds