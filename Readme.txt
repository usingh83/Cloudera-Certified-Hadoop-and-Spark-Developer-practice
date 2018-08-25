Problem Scenario 1 : You have been given below code snippet.
val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
Operation_xyz
Write a correct code snippet for Operation_xyz which will produce below output.
scala.collection.Map[lnt,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> S, 2 -> 3, 4 -> 2, 7 ->1)

Solution:-
Explanation: Solution : b.countByValue(). (Warning: This operation will finally aggregate the information in a single reducer.)

Problem Scenario 2 : You have been given MySQL DB with following details. You have been given following product.csv file
product.csv
productID,productCode,name,quantity,price
1001,PEN,Pen Red,5000,1.23
1002,PEN,Pen Blue,8000,1.25
1003,PEN,Pen Black,2000,1.25
1004,PEC,Pencil 2B,10000,0.48
1005,PEC,Pencil 2H,8000,0.49
1006,PEC,Pencil HB,0,9999.99
Now accomplish following activities.
1. Create a Hive ORC table using SparkSql
2. Load this data in Hive table.
3. Create a Hive parquet table using SparkSQL and load data in it.

Solution:-
Step 1 : Create this tile in HDFS under following directory /user/cloudera/he/exam/task1/product.csv 

touch product.csv
vi product.csv
hdfs dfs -mkdir /user/cloudera/he
hdfs dfs -mkdir /user/cloudera/he/exam
hdfs dfs -mkdir /user/cloudera/he/exam/task1
hdfs dfs -put product.csv /user/cloudera/he/exam/task1/
Step 2 : Now using Spark-shell read the file as RDD //

val products = sc.textFile("/user/cloudera/he/exam/task1/product.csv")
products.first()
case class Product(productid: Integer, code: String, name: String, quantity:Integer, price: Float) 
val prdRDD = products.filter(x=>x.split(",")(0)!="productID").map(_.split(",")).map(p => Product(p(0).toInt,p(1),p(2),p(3).toInt,p(4).toFloat)) 
prdRDD.first() 
prdRDD.count() 
val prdDF = prdRDD.toDF() 
Step 6 : 

import org.apache.spark.sql.SaveMode 
prdDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("product_orc_table") 
step 7:
Now create table using data stored in warehouse directory. With the help of hive. 

CREATE EXTERNAL TABLE products (productid int,code string,name string ,quantity int, price float) STORED AS orc LOCATION '/user/hive/warehouse/product_orc_table'; 
Step 8 : Now create a parquet table 

import org.apache.spark.sql.SaveMode 
prdDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("product_parquet_table") 
Step 9 : Now create table using this 

CREATE EXTERNAL TABLE products_parquet (productid int,code string,name string .quantity int, price float} STORED AS parquet LOCATION '/user/hive/warehouse/product_parquet_table'; 
Step 10 : Check data has been loaded or not. 


Select * from products; Select * from products_parquet;

Problem Scenario 3 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Now accomplish following activities.
1. Import departments table from mysql to hdfs as textfile in departments_text directory.
2. Import departments table from mysql to hdfs as sequncefile in departments_sequence
directory.
3. Import departments table from mysql to hdfs as avro file in departments avro directory.
4. Import departments table from mysql to hdfs as parquet file in departments_parquet
directory.

Solution:
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments --target-dir=/user/cloudera/departments_text --as-textfile
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments --target-dir=/user/cloudera/departments_sequence --as-sequencefile
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments --target-dir=/user/cloudera/departments_avro --as-avrodatafile
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments --target-dir=/user/cloudera/departments_parquet --as-parquetfile

Problem Scenario 4 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2) val b =
a.keyBy(_.length)
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),
(3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle}}}

Solution: 
b.groupByKey.collect

Problem Scenario 5 : You have been given below code snippet.
val a = sc.parallelize(1 to 10, 3)
operation1
b.collect
Output 1
Array[lnt] = Array(2, 4, 6, 8,10)
operation2
Output 2
Array[lnt] = Array(1,2, 3)
Write a correct code snippet for operation1 and operation2 which will produce desired
output, shown above.

Solution: 
val b = a.filter(_%2==0).collect 
val c = a.filter(_ < 4).collect

Problem Scenario 6 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
val b = sc.parallelize(1 to a.count.tolnt, 2)
val c = a.zip(b)
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2>, (ant,5))

Solution:

c.sortByKey(false).collect

Problem Scenario 7:
You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
1. Connect MySQL DB and check the content of the tables.
2. Copy "retaildb.categories" table to hdfs, without specifying directory name.
3. Copy "retaildb.categories" table to hdfs, in a directory name "categories_target".
4. Copy "retaildb.categories" table to hdfs, in a warehouse directory name
"categories_warehouse".

Solution:

sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "select * from categories"
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table categories
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table categories --target-dir /user/cloudera/categories_target
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table categories --warehouse-dir /user/cloudera/categories_warehouse

Problem Scenario 8 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x))
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, String}] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))

Solution:

val c=b.reduceByKey((a,b)=>a+b).collect

Problem Scenario 9 : You have been given below patient data in csv format,
patientID,name,dateOfBirth,lastVisitDate
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21
Accomplish following activities.
1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
2. Find all the patients who born in 2011
3. Find all the patients age
4. List patients whose last visited more than 60 days ago
5. Select patients 18 years old or younger

Solution
touch patient.csv
vi patient.csv
hdfs dfs -put -f patient.csv /user/root/

val a=sc.textFile("/user/root/patient.csv")
case class Patient(patientID:Int ,name: String ,dateOfBirth: String,lastVisitDate: String)
val b=a.map(x=>x.split(",")).filter(x=>x(0)!="patientID").map(x=>Patient(x(0).toInt,x(1),x(2),x(3)))
val c=b.toDF
c.registerTempTable("Patients")
val d=sqlContext.sql("select * from Patients where To_Date(CAST(UNIX_TIMESTAMP(lastVisitDate,'yyyy-MM-dd') as TIMESTAMP)) between '2012-09-15' and current_timestamp() ");

val d=sqlContext.sql("select * from Patients where YEAR(To_Date(CAST(UNIX_TIMESTAMP(dateOfBirth,'yyyy-MM-dd') as TIMESTAMP)))=2011 ");

val d=sqlContext.sql("select name,Cast(datediff(current_timestamp(),To_Date(CAST(UNIX_TIMESTAMP(dateOfBirth,'yyyy-MM-dd') as TIMESTAMP)))/365 as INT) from Patients ");

val d=sqlContext.sql("select * from Patients where datediff(current_timestamp(),To_Date(CAST(UNIX_TIMESTAMP(lastVisitDate,'yyyy-MM-dd') as TIMESTAMP)))>60");

val d=sqlContext.sql("select * from Patients where Cast(datediff(current_timestamp(),To_Date(CAST(UNIX_TIMESTAMP(dateOfBirth,'yyyy-MM-dd') as TIMESTAMP)))/365 as INT)<=18 ");


Problem Scenario 10 : You have been given below Python code snippet, with intermediate
output.
We want to take a list of records about people and then we want to sum up their ages and
count them.
So for this example the type in the RDD will be a Dictionary in the format of {name: NAME,
age:AGE, gender:GENDER}.
The result type will be a tuple that looks like so (Sum of Ages, Count)
people = []
people.append({'name':'Amit', 'age':45,'gender':'M'})
people.append({'name':'Ganga', 'age':43,'gender':'F'})
people.append({'name':'John', 'age':28,'gender':'M'})
people.append({'name':'Lolita', 'age':33,'gender':'F'})
people.append({'name':'Dont Know', 'age':18,'gender':'T'})
peopleRdd=sc.parallelize(people) //Create an RDD
peopleRdd.aggregate((0,0), seqOp, combOp) //Output of above line : 167, 5)
Now define two operation seqOp and combOp , such that
seqOp : Sum the age of all people as well count them, in each partition. combOp :
Combine results from all partitions.

Solution:

peopleRdd.aggregate((0,0),lambda acc,a:(acc[0]+a['age'],acc[1]+1) ,lambda acc1,acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1]))

sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "Insert into departments values(9999, '\"Data Science\"1');"

sqoop import--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments --enclosed-by \\' --fields-terminated-by \- --lines-terminated-by \:

Problem Scenario 11 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
1. In mysql departments table please insert following record. Insert into departments
values(9999, '"Data Science"1);
2. Now there is a downstream system which will process dumps of this file. However,
system is designed the way that it can process only files if fields are enlcosed in(') single
quote and separate of the field should be (-} and line needs to be terminated by : (colon).
3. If data itself contains the " (double quote } than it should be escaped by \.
4. Please import the departments table in a directory called departments_enclosedby and
file should be able to process by downstream system.

Solution:

sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "select * from departments"

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments --target-dir /user/cloudera/department_enclosed --enclosed-by "\\'" --fields-terminated-by - --lines-terminated-by :

Problem Scenario 12 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following.
1. Create a table in retailedb with following definition.
CREATE table departments_new (department_id int(11), department_name varchar(45),
created_date T1MESTAMP DEFAULT NOW());
2. Now isert records from departments table to departments_new
3. Now import data from departments_new table to hdfs.
4. Insert following 5 records in departmentsnew table. Insert into departments_new
values(110, "Civil" , null); Insert into departments_new values(111, "Mechanical" , null);
Insert into departments_new values(112, "Automobile" , null); Insert into departments_new
values(113, "Pharma" , null);
Insert into departments_new values(114, "Social Engineering" , null);
5. Now do the incremental import based on created_date column.

Solution:


sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "CREATE table departments_new (department_id int(11), department_name varchar(45),created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"

sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "Insert into departments_new (department_id, department_name) select * from departments;"

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments_new --target-dir /user/cloudera/departments_incremental

Insert into departments_new values(110, "Civil" , null);
Insert into departments_new values(111, "Mechanical" , null);
Insert into departments_new values(112, "Automobile" , null);
Insert into departments_new values(113, "Pharma" , null);
Insert into departments_new values(114, "Social Engineering" , null);

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table departments_new --target-dir /user/cloudera/departments_incremental --append --check-column created_date --incremental append --split-by department_id --last-value "2018-05-22 21:56:50"

Problem Scenario 13 : You have given three files as below.
spark3/sparkdir1/file1.txt
spark3/sparkd ir2ffile2.txt
spark3/sparkd ir3Zfile3.txt
Each file contain some text.
spark3/sparkdir1/file1.txt
Apache Hadoop is an open-source software framework written in Java for distributed
storage and distributed processing of very large data sets on computer clusters built from
commodity hardware. All the modules in Hadoop are designed with a fundamental
assumption that hardware failures are common and should be automatically handled by the
framework
spark3/sparkdir2/file2.txt
The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File
System (HDFS) and a processing part called MapReduce. Hadoop splits files into large
blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers
packaged code for nodes to process in parallel based on the data that needs to be
processed.
spark3/sparkdir3/file3.txt
his approach takes advantage of data locality nodes manipulating the data they have
access to to allow the dataset to be processed faster and more efficiently than it would be
in a more conventional supercomputer architecture that relies on a parallel file system
where computation and data are distributed via high-speed networking
Now write a Spark code in scala which will load all these three files from hdfs and do the
word count by filtering following words. And result should be sorted by word count in
reverse order.
Filter words ("a","the","an", "as", "a","with","this","these","is","are","in", "for",
"to","and","The","of")
Also please make sure you load all three files as a Single RDD (All three files must be
loaded using single API call).
You have also been given following codec
import org.apache.hadoop.io.compress.GzipCodec
Please use above codec to compress file, while saving in hdfs.

Solution:

val a=sc.textFile("/user/cloudera/1.txt,/user/cloudera/2.txt,/user/cloudera/3.txt")
val b=a.flatMap(line=> line.split(" ")).map(word=>word.trim())
val c=sc.parallelize(List("a","the","an", "as", "a","with","this","these","is","are","in", "for","to","and","The","of"))
val d=b.subtract(c)
val e=d.map(word=>(word,1)).reduceByKey(_+_)
val f=e.map(word=> word.swap)
val g=f.sortByKey(false)
g.saveAsTextFile("/user/cloudera/words")
import org.apache.hadoop.io.compress.GzipCodec
g.saveAsTextFile("/user/cloudera/words_compressed", classOf[GzipCodec])

Problem Scenario 14 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of products table : (product_id | product categoryid | product_name |
product_description | product_prtce | product_image )
Please accomplish following activities.
1. Copy "retaildb.products" table to hdfs in a directory p93_products
2. Filter out all the empty prices
3. Sort all the products based on price in both ascending as well as descending order.
4. Sort all the products based on price as well as product_id in descending order.
5. Use the below functions to do data ordering or ranking and fetch top 10 elements top()
takeOrdered() sortByKey()

Solution:

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table products --target-dir /user/cloudera/p93_products 

val a=sc.textFile("/user/cloudera/p93_products")
val b=a.map(x=>x.split(",")).filter(y=>y(4).length!=0 && y(4).toFloat>0.0).map(l=>(l(0).toInt,l(1).toInt,l(2),l(3),l(4).toFloat,l(5)))
val c=b.map(x=>(x._5,x)).sortByKey(numPartitions=1)
val d=b.map(x=>(x._5,x)).sortByKey(false,numPartitions=1)
val e=b.map(x=>((x._5,x._1),x)).sortByKey(false,numPartitions=1)
val f=b.map(x=>((x._5,x._1),x)).top(10)
val g=b.map(x=>((x._5,x._1),x)).takeOrdered(10)(Ordering[((Float,Int),(Int,Int,String,String,Float,String))].reverse)
val h=b.map(x=>((x._5,x._1),x)).sortByKey(false,numPartitions=1).take(10)

Problem Scenario 15 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.products
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of products table : (product_id | product_category_id | product_name | product_description | product_price | product_image )
Please accomplish following activities.
1. Copy "retaildb.products" table to hdfs in a directory p93_products
2. Now sort the products data sorted by product price per category, use productcategoryid
colunm to group by category

Solution:

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table products --target-dir /user/cloudera/p93_products 
val a=sc.textFile("/user/cloudera/p93_products")
val b=a.map(x=>x.split(",")).filter(y=>y(4).length!=0 && y(4).toFloat>0.0).map(l=>(l(0).toInt,l(1).toInt,l(2),l(3),l(4).toFloat,l(5)))
case class Persons(id: Int, name: String,desc: String, price: Float, image: String);
val c=b.map(x=>(x._2,Persons(x._1,x._3,x._4,x._5,x._6))).groupByKey()
val d=c.map(x=>(x._1,x._2.toList.sortWith(_.price<_.price).toIterable))

Problem Scenario 16 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following.
1. Create a database named hadoopexam and then create a table named departments in
it, with following fields. department_id int,
department_name string
e.g. location should be
hdfs://quickstart.cloudera:8020/user/hive/warehouse/hadoopexam.db/departments
2. Please import data in existing table created above from retaidb.departments into hive
table hadoopexam.departments.
3. Please import data in a non-existing table, means while importing create hive table
named hadoopexam.departments_new

Solution:

create database hadoopexam;
use hadoopexam;
create table departments(department_id int, department_name string) ;
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera --table departments --hive-import --hive-table hadoopexam.departments

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera --table departments --hive-import --hive-table hadoopexam.departments_new --create-hive-table

Problem Scenario 17 : You need to implement near real time solutions for collecting
information when submitted in file with below
Data
echo "IBM,100,20160104" >> /tmp/spooldir2/.bb.txt
echo "IBM,103,20160105" >> /tmp/spooldir2/.bb.txt
mv /tmp/spooldir2/.bb.txt /tmp/spooldir2/bb.txt
After few mins
echo "IBM,100.2,20160104" >> /tmp/spooldir2/.dr.txt
echo "IBM,103.1,20160105" >> /tmp/spooldir2/.dr.txt
mv /tmp/spooldir2/.dr.txt /tmp/spooldir2/dr.txt
You have been given below directory location (if not available than create it) /tmp/spooldir2
As soon as file committed in this directory that needs to be available in hdfs in
/tmp/flume/primary as well as /tmp/flume/secondary location.
However, note that/tmp/flume/secondary is optional, if transaction failed which writes in
this directory need not to be rollback.
Write a flume configuration file named flumeS.conf and use it to load data in hdfs with
following additional properties .
1. Spool /tmp/spooldir2 directory
2. File prefix in hdfs sholuld be events
3. File suffix should be .log
4. If file is not committed and in use than it should have _ as prefix.
5. Data should be written as text to hdfs

Flume

Problem Scenario 95 : You have to run your Spark application on yarn with each executor
Maximum heap size to be 512MB and Number of processor cores to allocate on each
executor will be 1 and Your main application required three values as input arguments V1
V2 V3.
Please replace XXX, YYY, ZZZ
./bin/spark-submit -class com.hadoopexam.MyTask --master yarn-cluster--num-executors 3
--driver-memory 512m XXX YYY lib/hadoopexam.jarZZZ

solution:

./bin/spark-submit -class com.hadoopexam.MyTask --master yarn-cluster--num-executors 3 --driver-memory 512m -executor-memory 512m -executor-cores 1 lib/hadoopexam.jar V1 V2 V3

Problem Scenario 50 : You have been given below code snippet (calculating an average
score}, with intermediate output.
type ScoreCollector = (Int, Double)
type PersonScores = (String, (Int, Double))
val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()
val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner,
scoreMerger)
val averagingFunction = (personScore: PersonScores) =>  val (name, (numberScores,totalScore)) = personScore (name, totalScore / numberScores) 
val averageScores = scores.collectAsMap().map(averagingFunction)
Expected output: averageScores: scala.collection.Map[String,Double] = Map(Fred ->
91.33333333333333, Wilma -> 95.33333333333333)
Define all three required function , which are input for combineByKey method, e.g.
(createScoreCombiner, scoreCombiner, scoreMerger). And help us producing required
results.

Solution:

val scores = wilmaAndFredScores.combineByKey((a:Double)=>(1,a), (acc1:ScoreCollector,a:Double)=> (acc1._1+1,acc1._2+a), (acc1:ScoreCollector,acc2:ScoreCollector)=>(acc1._1+acc2._1,acc1._2+acc2._2))

Problem Scenario 83 : In Continuation of previous question, please accomplish following
activities.
1. Select all the records with quantity >= 5000 and name starts with 'Pen'
2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
4. Select all the products which name is 'Pen Red', 'Pen Black'
5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.

Solution:

Problem Scenario 46 : You have been given below list in scala (name,sex,cost) for each
work done.
List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",
2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
Now write a Spark program to load this list as an RDD and do the sum of cost for
combination of name and sex (as key)

Solution:

val a=sc.parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)))
val b=a.map(x=>((x._1,x._2),x._3))
val c=b.reduceByKey(_+_)
c.repartition(1).saveAsTextFile("/spark12")

Problem Scenario 41 : You have been given below code snippet.
val aul = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
Apply the Spark method, which will generate below output.
Array[(String, Array[lnt])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a(Array(3)), (b,Array(2)))

Solution:

val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
val b=au1.union(au2)
b.collect

Problem Scenario 60 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"}, 3}
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","woif","bear","bee"), 3)
val d = c.keyBy(_.length)
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)),
(6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)),
(6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)),
(3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))

Solution:

val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","woif","bear","bee"), 3)
val d = c.keyBy(_.length)
val e=b.join(d).collect

Problem Scenario 13 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following.
1. Create a table in retailedb with following definition.
CREATE table departments_export (department_id int(11), department_name varchar(45),created_date T1MESTAMP DEFAULT NOW());
2. Now import the data from following directory into departments_export table, /user/cloudera/departments new

Solution:


sqoop eval --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "CREATE table departments_export (department_id int(11), department_name varchar(45),created_date TIMESTAMP DEFAULT NOW());"

sqoop export --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera --export-dir /user/cloudera/departments_incremental --table departments_export

Problem Scenario 23 : You have been given log generating service as below.
Start_logs (It will generate continuous logs)
Tail_logs (You can check , what logs are being generated)
Stop_logs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flume3.conf , using that configuration file dumps
logs in HDFS file system in a directory called flumeflume3/%Y/%m/%d/%H/%M
Means every minute new directory should be created). Please us the interceptors to
provide timestamp information, if message header does not have header info.
And also note that you have to preserve existing timestamp, if message contains it. Flume
channel should have following property as well. After every 100 message it should be
committed, use non-durable/faster channel and it should be able to hold maximum 1000
events.

Flume

Problem Scenario 16 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish below assignment.
1. Create a table in hive as below.
create table departments_hive(department_id int, department_name string);
2. Now import data from mysql table departments to this hive table. Please make sure that
data should be visible using below hive command, select" from departments_hive

Solution:

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera --table departments --hive-import --hive-table departments_hive

Problem Scenario 92 : You have been given a spark scala application, which is bundled in
jar named hadoopexam.jar.
Your application class name is com.hadoopexam.MyTask
You want that while submitting your application should launch a driver on one of the cluster
node.
Please complete the following command to submit the application.
spark-submit XXX -master yarn YYY SSPARK HOME/lib/hadoopexam.jar 10

Solution:

spark-submit --class com.hadoopexam.MyTask --master yarn --deploy-mode cluster HOME/lib/hadoopexam.jar 10

Problem Scenario 43 : You have been given following code snippet.
val grouped = sc.parallelize(Seq(((1,"twoM), List((3,4), (5,6)))))
val flattened = grouped.flatMap {A => groupValues.map { value => B }}
You need to generate following output.
Hence replace A and B
Array((1,two,3,4),(1,two,5,6))

Solution:

val flattened = grouped.flatMap {case (key,groupValues) => groupValues.map { value => (key,value) }}

Problem Scenario 61 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length) 
operationl
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, (String, Option[String]}}] = Array((6,(salmon,Some(salmon))),
(6,(salmon,Some(rabbit))),
(6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
(6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))),
(3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat)j),
(3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))

Solution:

val e=b.leftOuterJoin(d)

Problem Scenario 94 : You have to run your Spark application on yarn with each executor
20GB and number of executors should be 50. Please replace XXX, YYY, ZZZ
export HADOOP_CONF_DIR=XXX
./bin/spark-submit \
-class com.hadoopexam.MyTask \
xxx\
-deploy-mode cluster \ # can be client for client mode
YYY\
ZZZ \
/path/to/hadoopexam.jar \
1000

Solution:

./bin/spark-submit \
-class com.hadoopexam.MyTask \
--master yarn \
-deploy-mode cluster \ # can be client for client mode
--executor-memory 2g \
--executor-cores 1 \
/path/to/hadoopexam.jar \
1000

Problem Scenario 4: You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
Import Single table categories (Subset data} to hive managed table , where category_id
between 1 and 22

Solution:


sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera --table categories --hive-import --hive-table categories_hive --create-hive-table --where "category_id between 1 and 22"

Problem Scenario 17 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish below assignment.
1. Create a table in hive as below, create table departments_hive01(department_id int, department_name string, avg_salary int);
2. Create another table in mysql using below statement CREATE TABLE IF NOT EXISTS
departments_hive01(id int, department_name varchar(45), avg_salary int);
3. Copy all the data from departments table to departments_hive01 using insert into
departments_hive01 select a.*, null from departments a;
Also insert following records as below
insert into departments_hive01 values(777, "Not known",1000);
insert into departments_hive01 values(8888, null,1000);
insert into departments_hive01 values(666, null,1100);
4. Now import data from mysql table departments_hive01 to this hive table. Please make
sure that data should be visible using below hive command. Also, while importing if null
value found for department_name column replace it with "" (empty string) and for id column
with -999 select * from departments_hive;

Solution:

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera --table departments_hive01 --hive-import --hive-table departments_hive01 --null-string "" --hive-overwrite --split-by id

Problem Scenario 21 : You have been given log generating service as below.
startjogs (It will generate continuous logs)
tailjogs (You can check , what logs are being generated)
stopjogs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flumel.conf , using that configuration file dumps
logs in HDFS file system in a directory called flumel. Flume channel should have following
property as well. After every 100 message it should be committed, use non-durable/faster
channel and it should be able to hold maximum 1000 events
Solution :
Step 1 : Create flume configuration file, with below configuration for source, sink and
channel.
#Define source , sink , channel and agent,
agent1 .sources = source1
agent1 .sinks = sink1
agent1.channels = channel1
# Describe/configure source1
agent1 .sources.source1.type = exec
agent1.sources.source1.command = tail -F /opt/gen logs/logs/access.log
## Describe sinkl
agentl .sinks.sinkl.channel = memory-channel
agentl .sinks.sinkl .type = hdfs
agentl .sinks.sink1.hdfs.path = flumel
agentl .sinks.sinkl.hdfs.fileType = Data Stream
# Now we need to define channell property.
agent1.channels.channel1.type = memory
agent1.channels.channell.capacity = 1000
agent1.channels.channell.transactionCapacity = 100
# Bind the source and sink to the channel
agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1
Step 2 : Run below command which will use this configuration file and append data in
hdfs.
Start log service using : startjogs
Start flume service:
flume-ng agent -conf /home/cloudera/flumeconf -conf-file
/home/cloudera/flumeconf/flumel.conf-Dflume.root.logger=DEBUG,INFO,console
Wait for few mins and than stop log service.
Stop_logs

flume:

Problem Scenario 67 : You have been given below code snippet.
lines = sc.parallelize(['lts fun to have fun,','but you have to know how.'])
r1 = lines.map( lambda x: x.replace(',','').replace('.','').lower())
r2 = r1.flatMap(lambda x: x.split())
r3 = r2.map(lambda x: (x, 1))
operation1
r5 = r4.map(lambda x:(x[1],x[0]))
r6 = r5.sortByKey(ascending=False)
r6.take(20)
Write a correct code snippet for operationl which will produce desired output, shown below.
[(2, 'fun'), (2, 'to'), (2, 'have'), (1, its'), (1, 'know1), (1, 'how1), (1, 'you'), (1, 'but')]

Solution:

r4=r3.reduceByKey(lambda x,y:x+y)

Problem Scenario 25 : You have been given below comma separated employee
information. That needs to be added in /home/cloudera/flumetest/in.txt file (to do tail
source)
sex,name,city
1,alok,mumbai
1,jatin,chennai
1,yogesh,kolkata
2,ragini,delhi
2,jyotsana,pune
1,valmiki,banglore
Create a flume conf file using fastest non-durable channel, which write data in hive
warehouse directory, in two separate tables called flumemaleemployee1 and
flumefemaleemployee1
(Create hive table as well for given data}. Please use tail source with
/home/cloudera/flumetest/in.txt file.
Flumemaleemployee1 : will contain only male employees data flumefemaleemployee1 :
Will contain only woman employees data

flume

Problem Scenario 37 : ABCTECH.com has done survey on their Exam Products feedback
using a web based form. With the following free text field as input in web ui.
Name: String
Subscription Date: String
Rating : String
And servey data has been saved in a file called spark9/feedback.txt
Christopher|Jan 11, 2015|5
Kapil|11 Jan, 2015|5
Thomas|6/17/2014|5
John|22-08-2013|5
Mithun|2013|5
Jitendra||5
Write a spark program using regular expression which will filter all the valid dates and save
in two separate file (good record and bad record)

Regex:

Problem Scenario GG : You have been given below code snippet.
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
val d = c.keyBy(.length)
operation 1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, String)] = Array((4,lion))

Solution:

val e=b.substractByKey(d).collect

Problem Scenario 22 : You have been given below comma separated employee
information.
name,salary,sex,age
alok,100000,male,29
jatin,105000,male,32
yogesh,134000,male,39
ragini,112000,female,35
jyotsana,129000,female,39
valmiki,123000,male,29
Use the netcat service on port 44444, and nc above data line by line. Please do the
following activities.
1. Create a flume conf file using fastest channel, which write data in hive warehouse
directory, in a table called flumeemployee (Create hive table as well tor given data).
2. Write a hive query to read average salary of all employees.

Flume:

Problem Scenario 8 : You have been given following mysql database details as well as
other info.
Please accomplish following.
1. Import joined result of orders and order_items table join on orders.order_id =
order_items.order_item_order_id.
2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
3. Also make sure you use orderid columns for sqoop to use for boundary conditions.

Solution:

sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --query "select * from orders,order_items where orders.order_id =order_items.order_item_order_id and \$CONDITIONS" --target-dir "/user/cloudera/orders_order_items" -m 2 --split-by order_id

Problem Scenario 70 : Write down a Spark Application using Python, In which it read a
file "Content.txt" (On hdfs) with following content. Do the word count and save the
results in a directory called "problem85" (On hdfs)
Content.txt
Hello this is ABCTECH.com
This is XYZTECH.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce

Solution:

val a=sc.textFile("/user/cloudera/content.txt")
val b=a.flatMap(x=> x.split(" ")).map(y=> (y.trim(),1))
val c=b.reduceByKey(_+_)

Problem Scenario 85 : In Continuation of previous question, please accomplish following
activities.
1. Select all the columns from product table with output header as below. productID AS ID
code AS Code name AS Description price AS 'Unit Price'
2. Select code and name both separated by ' -' and header name should be Product
Description'.
3. Select all distinct prices.
4. Select distinct price and name combination.
5. Select all price data sorted by both code and productID combination.
6. count number of products.
7. Count number of products for each code.

?

Problem Scenario 77 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of order table : (orderid , order_date , order_customer_id, order_status)
Columns of ordeMtems table : (order_item_id , order_item_order_ld ,
order_item_product_id, order_item_quantity,order_item_subtotal,order_
item_product_price)
Please accomplish following activities.
1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory
p92_orders and p92 order items .
2. Join these data using orderid in Spark and Python
3. Calculate total revenue perday and per order
4. Calculate total and average revenue for each date. - combineByKey
-aggregateByKey

Solution:


sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table orders --target-dir "/user/cloudera/92_orders"


sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username retail_dba --password cloudera --table order_items --target-dir "/user/cloudera/92_order_items"

val orders=sc.textFile("/user/cloudera/92_orders")
val order_items=sc.textFile("/user/cloudera/92_order_items")

case class Orders(orderid: Int , order_date: String , order_customer_id: Int, order_status: String)
case class Order_items(order_item_id: Int , order_item_order_id: Int ,order_item_product_id: Int, order_item_quantity: Int,order_item_subtotal: Float,order_item_product_price: Float)
val ordersDF=orders.map(x=> x.split(",")).map(y=> Orders(y(0).toInt,y(1),y(2).toInt,y(3))).toDF
val order_itemsDF=order_items.map(x=> x.split(",")).map(y=> Order_items(y(0).toInt,y(1).toInt,y(2).toInt,y(3).toInt,y(4).toFloat,y(5).toFloat)).toDF
ordersDF.registerTempTable("orders")
order_itemsDF.registerTempTable("order_items")
val join=sqlContext.sql("select * from orders,order_items where orders.orderid=order_items.order_item_order_id")
val ravperdayperorder=sqlContext.sql("select orderid,order_date,sum(order_item_subtotal) from orders,order_items where orders.orderid=order_items.order_item_order_id group by orderid,order_date")

val ord=orders.map(x=> x.split(",")).map(y=> (y(0).toInt,y(1)))
val orditm=order_items.map(x=> x.split(",")).map(y=> (y(1).toInt,y(4).toFloat))
val jordorditm=ord.join(orditm)
val ires=jordorditm.map(x=> (x._2._1,x._2._2))

type a=(Float,Int)
val res=ires.combineByKey((a:Float)=>(a,1),(acc:a,b)=>(acc._1+b,acc._2+1), (acc1:a,acc2:a)=>(acc1._1+acc2._1,acc1._2+acc2._2))
val fres=res.map(x=> (x._1,x._2._1,x._2._1/x._2._2))

val ares=ires.aggregateByKey((0.0,0))((acc,b)=>(acc._1+b,acc._2+1), (acc1,acc2)=>(acc1._1+acc2._1,acc1._2+acc2._2))
val afres=ares.map(x=> (x._1,x._2._1,x._2._1/x._2._2))

Problem Scenario 49 : You have been given below code snippet (do a sum of values by
key}, with intermediate output.
val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C","bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList}
//Create key value pairs
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
val initialCount = 0;
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
Now define two functions (addToCounts, sumPartitionCounts) such, which will
produce following results.
Output 1
countByKey.collect
res3: Array[(String, Int)] = Array((foo,5), (bar,3))
import scala.collection._
val initialSet = scala.collection.mutable.HashSet.empty[String]
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
Now define two functions (addToSet, mergePartitionSets) such, which will produce
following results.
Output 2:
uniqueByKey.collect
res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A}},
(bar,Set(C, D}}}

Solution:

val countByKey = kv.aggregateByKey((0))((acc,a)=>(acc+1), (acc1,acc2)=> (acc1+acc2))

val uniqueByKey = kv.aggregateByKey(initialSet)((acc,a)=>(acc+a), (acc1,acc2)=> (acc1.union(acc2)))


