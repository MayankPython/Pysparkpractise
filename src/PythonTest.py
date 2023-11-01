"""
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("Testspark")\
    .getOrCreate()

mylist = [("Raj", "123" , "456"),
          ("Rahul","234","234"),
          ("Ratan", "345" , "498"),
          ("Remo" , "598", "298")
          ]

df = spark.createDataFrame(mylist).toDF("name", "home_phone", "work_phone")
df.createOrReplaceTempView("temp_view")
df1 = spark.sql("select name, home_phone from temp_view")
df2 = spark.sql("select name, work_phone from temp_view")
join_condition = df1.name == df2.name
df3 = df1.union(df2).sort(df1.name).withColumnRenamed("home_phone", "phone").show()
#df3.dropDuplicates().show()
"""


#pyramid = int(input("enter the pyramid base: "))
"""
def right_trangle(n):

    mylist = []
    for i in range(1, n+1):
        mylist.append("* "*i)

    for j in mylist:
        print(j)

pyramid = 5       
right_trangle(pyramid)
"""

"""
def left_trangle(n):
    k = 2*n - 2
    for i in range(0, n):

        for j in range(0, k):
            print(end=" ")

        k -= 2

        for x in range(0,i+1):
            print(" *", end="")

        print("\r")

n = 5
left_trangle(n)
"""

"""
def full_trangle(n):
    k = 2*n-2
    for i in range(1, n+1):
        for j in range(0, k):
            print(end=" ")
        k -= 2
        for x in range(0, (i*2-1)):
            print(" *", end="")

        print("\r")

n = 5
full_trangle(n)
"""
"""
def pyramid(n):
    k = 2*n - 2
    count = 1
    for i in range(0, n):
        for j in range(0, k):
            print(end=" ")
        k -= 2

        print(" *")
        for x in range(0, count):
            print(end="  ")

        if count > 1:
            print(" *")
        count += 2

n = 5
pyramid(n)
"""
"""
def fibonacci(n):
    a = 0
    b = 1
    print(a,",", b, end=",")
    for i in range(1, n):
        c = a+b
        a = b
        b = c
        print(c, end=",")


fibonacci(12)
"""
"""
thislist = ["apple", "banana", "cherry"]
templist = []
for i in range(0,len(thislist)):
    templist.append(thislist.pop())

print(templist)
"""
"""
list_one = ['January', 'February', 'March']
print(list_one)
list_one.append(['April', 'May', 'June'])
print(list_one)
list_one.extend(['July', 'Aug', 'Sep'])
print(list_one)
"""
"""
def simpleGeneratorFun():
    for i in range(0,10):
        yield i

for value in simpleGeneratorFun():
    print(value)
"""
"""
def myfunc(n):
  return abs(n - 50)

thislist = [100, 50, 65, 82, 23]
thislist.sort(key = myfunc)
print(thislist)
"""
"""
thisdict = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}
x = thisdict.keys()
a = ""
for i in x:
    a= a+i
print(a)
"""
"""
a = "bubble"
thisdict = dict()
for i in a:
    if i not in thisdict:
        thisdict[i] = ""
thislist = thisdict.keys()
for x in thislist:
    print(x, end="")
"""
"""
numbers = [1,2,3,4]
result = isinstance(numbers, tuple)
print(result)
"""
"""
thislist = [11, '5', 17,'10','python', 18, 23]
total = 0
for i in thislist:
    if isinstance(i, int):
        total += i
    elif isinstance(i, str):
        if i.isnumeric():
            total += int(i)

print(total)
"""
"""
def myfunc(x):
    return x[-1]
thislist = ['April', 'May', 'June']
thislist.sort(key=myfunc)
print(thislist)
"""
"""
x = "hello"
assert x == "hello"
"""
"""
def my_function(*kids):
#kids is received as tuple.
  print("The youngest child is " + kids[2])

my_function("Emil", "Tobias", "Linus")

"""
"""
def my_function(**kid):
  #kid is received as dictionary.
  print("His last name is " + kid["lname"])

my_function(fname = "Tobias", lname = "Refsnes")

"""
"""
def filter_odd(numbers):

   for number in range(numbers):

       if(number%2!=0):

           yield number

odd_numbers = filter_odd(20)
print(odd_numbers)
print(list(odd_numbers))

"""

import os

os.environ