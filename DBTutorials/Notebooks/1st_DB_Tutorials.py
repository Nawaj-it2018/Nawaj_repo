# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/default/sales/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading from CSV

# COMMAND ----------

spark.read.format('csv').option('inferscehma',True).option('header',True).load('/Volumes/workspace/default/sales/BigMart Sales.csv').display()

# COMMAND ----------

df=spark.read.format('csv').option('inferscehma',True).option('header',True).load('/Volumes/workspace/default/sales/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema Definition 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema='''
Item_Identifier STRING,
Item_Weight DOUBLE,
Item_Fat_Content STRING,
Item_Visibility DOUBLE,
Item_Type STRING,
Item_MRP DOUBLE,
Outlet_Identifier STRING,
Outlet_Establishment_Year STRING,
Outlet_Size STRING,
Outlet_Location_Type STRING,
Outlet_Type STRING,
Item_Outlet_Sales STRING
'''


# COMMAND ----------

df=spark.read.format('csv').schema(my_ddl_schema).option('header',True).load('/Volumes/workspace/default/sampleread/sales/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

my_struct_schema=StructType([
                            StructField('Item_Identifier',StringType(),True),
                            StructField('Item_Weight',StringType(),True),
                            StructField('Item_Fat_Content',StringType(),True),
                            StructField('Item_Visibility',StringType(),True),
                            StructField('Item_Type',StringType(),True),
                            StructField('Item_MRP',StringType(),True),
                            StructField('Outlet_Identifier',StringType(),True),
                            StructField('Outlet_Establishment_Year',StringType(),True),
                            StructField('Outlet_Size',StringType(),True),
                            StructField('Outlet_Location_Type',StringType(),True),
                            StructField('Outlet_Type',StringType(),True),
                            StructField('Item_Outlet_Sales',StringType(),True)
])

# COMMAND ----------

df=spark.read.format('csv').schema(my_struct_schema).option('header',True).load('/Volumes/workspace/default/sales/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Select

# COMMAND ----------

df_select=df.select('Item_Identifier','Item_Weight','Item_Fat_Content','Item_Visibility')
df_select.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Select With Col

# COMMAND ----------

df_col_select=df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content'),col("Item_Visibility"))
df_col_select.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df_alias=df.select(col('Item_Identifier').alias('Item_Id'))
df_alias.display()

# COMMAND ----------

df_alias1=df.select(col('Item_Identifier').alias('Item_Id'),col("Item_Fat_Content").alias('Item_Fat_Level'),'Item_Visibility')
df_alias1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCENARIO-1

# COMMAND ----------

df_filter1=df.filter(col('Item_Fat_Content')=='Regular')
df_filter1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCENARIO-2

# COMMAND ----------

df_filter2=df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')>10))
df_filter2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCENARIO-3

# COMMAND ----------

df_filter3=df.filter((col("Outlet_Size").isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2')))
df_filter3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumnRenamed

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumnRenamed('Item_Identifier','Item_ID')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-1

# COMMAND ----------

df_newcol=df.withColumn('NewColName',lit('new'))
df_newcol.display()

# COMMAND ----------

df_newcol2=df_newcol.withColumn('Item_Weight*ITEM_MRP',col('Item_Weight')*col('Item_MRP'))
df_newcol2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-2

# COMMAND ----------

df_modify1=df_newcol2.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))
df_modify1.display()

# COMMAND ----------

df_modify2=df_modify1.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Reg","RF"))\
                        .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF"))

df_modify2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Cast

# COMMAND ----------

df_modify2.withColumn('Item_WT',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

df_modify2.withColumn('Item_Weight',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

df=df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort the data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-1

# COMMAND ----------

df_sort1=df.sort(col('Item_Weight').asc())
df_sort1.display()

# COMMAND ----------

df_sort2=df.sort(col('Item_Weight').desc())
df_sort2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sorting based on multiple Columns

# COMMAND ----------

df_sort3=df.sort(col('Item_Weight').asc(),col('Item_Visibility').asc())
df_sort3.display()

# COMMAND ----------

df_sort4=df.sort([col('Item_Weight').asc(),col('Item_Visibility').asc()])
df_sort4.display()

# COMMAND ----------

df_sort5=df.sort([col('Item_Weight'),col('Item_Visibility')],ascending=[0,0])
df_sort5.display()

# COMMAND ----------

df_sort6=df.sort([col('Item_Weight'),col('Item_Visibility')],ascending=[0,1])
df_sort6.display()

# COMMAND ----------

df_sort7=df.sort([col('Item_Weight'),col('Item_Visibility')],ascending=[1,0])
df_sort7.display()

# COMMAND ----------

df_sort8=df.sort([col('Item_Weight'),col('Item_Visibility')],ascending=[1,1])
df_sort8.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

df.drop(col('Item_Type')).display()

# COMMAND ----------

df.drop(col("Item_Type"),col("Item_Weight")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicate Records

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Identifier','Item_Weight','Item_Visibility','Item_Type',]).display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Identifier','Item_Weight','Item_Type',]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION and UNION BYNAME

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing DataFrames

# COMMAND ----------

data1=[('1','kad'),('2','sid')]
schema1='id STRING,name STRING'
df1=spark.createDataFrame(data1,schema1)

data2=[('3','rahul'),('4','jas')]
schema2='id STRING,name STRING'
df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION

# COMMAND ----------

# MAGIC %md
# MAGIC 1. order of columns in both dataframe is same

# COMMAND ----------

df_union1=df1.union(df2)
df_union1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. If order of column of one dataframe is not same with other one then it will mess the data.

# COMMAND ----------

data3=[('kad','1'),('SiD and das','2')]
schema3='name STRING,id STRING'
df3=spark.createDataFrame(data3,schema3)
df3.display()
data4=[('3','RaHul and kHAN'),('4','jAs')]
schema4='id STRING,name STRING'
df4=spark.createDataFrame(data4,schema4)
df4.display()

# COMMAND ----------

df_union3=df3.union(df4)
df_union3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### UNION BYNAME

# COMMAND ----------

df_unionbyname=df3.unionByName(df4)
df_unionbyname.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING FUNCTIONS

# COMMAND ----------

df_all=df3.unionByName(df4)
df_all.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### INITCAP()

# COMMAND ----------

df_all.select(initcap('name').alias("Name_InitCap")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOWER()

# COMMAND ----------

df_all.select('id',lower('name').alias('Name_Lower')).display()

# COMMAND ----------

df_all.select(upper('name').alias('Upper_Name'),'id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET DATE & TIME 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Current_Date()

# COMMAND ----------

df_date=df_union3.withColumn('Current_Date',current_date())

# COMMAND ----------

df_date.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date_ADD()

# COMMAND ----------

df_date_add=df_date.withColumn('Future_Date',date_add('Current_Date',7))
df_date_add.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATE_SUB()

# COMMAND ----------

df_date_sub=df_date_add.withColumn('Past_Date',date_sub('Current_Date',7))

# COMMAND ----------

df_date_sub.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATEDIFF()

# COMMAND ----------

df_date_diff=df_date_sub.withColumn('Date_Difference',datediff('Future_Date','Past_Date'))
df_date_diff.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATE_FORMAT()

# COMMAND ----------

df_date_format=df_date_diff.withColumn('Date_Format_Change_Current',date_format('Current_Date','dd-MM-yyyy'))
df_date_format.display()

# COMMAND ----------

df_timestamp=df_date_format.withColumn('Date_Time',current_timestamp())
df_timestamp.display()

# COMMAND ----------

df_timestamp1=df_timestamp.withColumn('Date_Time1',date_format(current_timestamp(),'dd-MM-yyyy HH:mm:ss'))
df_timestamp1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### HANDLING NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping NULLS

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Item_Weight']).display()

# COMMAND ----------

df.dropna(subset=['Item_Weight','Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filling NULLS

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('Not Available',subset=['Outlet_Size','Item_Weight']).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT and INDEXING

# COMMAND ----------

# MAGIC %md
# MAGIC #### SPLIT()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### INDEXING

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE()

# COMMAND ----------

df_exp=df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAY_CONTAINS()

# COMMAND ----------

df_exp.withColumn('Type1_Flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GROUP BY

# COMMAND ----------

df_gb=df.groupBy('Item_Type').agg(sum('Item_MRP').alias('Total_Item_MRP'))
df_gb.display()

# COMMAND ----------

df_gb1=df.groupBy('Item_Type').agg(sum('Item_MRP').alias('Total_Item_MRP'),avg('Item_MRP').alias('Average_Item_MRP'))
df_gb1.display()

# COMMAND ----------

df_gb2=df.groupBy('Item_Type').agg(max('Item_MRP').alias('Max_Item_MRP'),min('Item_MRP').alias('Min_Item_MRP'),sum('Item_MRP').alias('Total_Item_MRP'),avg('Item_MRP').alias('Average_Item_MRP'))
df_gb2.display()

# COMMAND ----------

df_gb3=df.groupBy('Item_Type','Outlet_Size').agg(max('Item_MRP').alias('Max_Item_MRP'),min('Item_MRP').alias('Min_Item_MRP'),sum('Item_MRP').alias('Total_Item_MRP'),avg('Item_MRP').alias('Average_Item_MRP'))
df_gb3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### COLLECT_LIST

# COMMAND ----------

data_book=[('user1','book1'),('user1','book2'),('user2','book3'),('user3','book1'),('user2','book4')]
schema_book='user STRING,book STRING'
df_book=spark.createDataFrame(data_book,schema_book)
df_book.display()

# COMMAND ----------

df_book_collect=df_book.groupBy('user').agg(collect_list('book'))
df_book_collect.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------

df_pivot=df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP'))
df_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN_OTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-1

# COMMAND ----------

df_when=df.withColumn('Veg Flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))
df_when.display()

# COMMAND ----------

df_when.groupBy('Veg Flag').agg(count('Veg Flag').alias('Total Item')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-2

# COMMAND ----------

#df_when=df.withColumn('Veg Flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))
df_when1=df_when.withColumn('Veg_exp_flag',when(((col('Veg Flag')=='Veg') & (col('Item_MRP')<100)),'Veg Inexpensive')\
                                        .when(((col('Veg Flag')=='Veg') & (col('Item_MRP')>=100)),'Veg Expensive')\
                                        .otherwise('Non-Veg'))
df_when1.display()

# COMMAND ----------

df_when1.select('Veg Flag').display()

# COMMAND ----------

df_when2=df_when.withColumn('Veg_exp_flag',when(((col('Veg Flag')=='Veg') & (col('Item_MRP')<100)),'Veg Inexpensive')\
                                        .when(((col('Veg Flag')=='Veg') & (col('Item_MRP')>=100)),'Veg Expensive')\
                                        .when(((col('Veg Flag')=='Non-Veg') & (col('Item_MRP')<100)),'Non-Veg Inexpensive')\
                                        .when(((col('Veg Flag')=='Non-Veg') & (col('Item_MRP')>=100)),'Non-Veg Expensive')\
                                        .otherwise('Not Available'))
df_when2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------

data_emp=[('1','Ajay','d001'),('2','Rahim','d002'),('3','Dsouza','d003'),('4','Ram','d003'),('5','Aman','d005'),('6','Sumon','d006'),('7','Nam','d003'),('8','Amar','d002')]
schema_emp='Id STRING, Name STRING, Dept_Id String'
df1=spark.createDataFrame(data_emp,schema_emp)
df1.display()
data_dept=[('d001','HR'),('d002','Marketing'),('d003','Accounts'),('d004','IT'),('d005','Finance')]
schema_dept='Dept_Id String, Dept_Name String'
df2=spark.createDataFrame(data_dept,schema_dept)
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. INNER JOIN()

# COMMAND ----------

df_inner=df1.join(df2,df1.Dept_Id==df2.Dept_Id,'inner')
df_inner.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. LEFT JOIN() 

# COMMAND ----------

df_left=df1.join(df2,df1.Dept_Id==df2.Dept_Id,'left')
df_left.display()
df_left.select('Id','Name',df1['Dept_Id'],'Dept_Name').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RIGHT JOIN()

# COMMAND ----------

df_right=df1.join(df2, df1['Dept_Id']==df2['Dept_Id'],'right')
df_right.display()
df_right.select('Id','Name',df2["Dept_Id"],"Dept_Name").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOIN()
# MAGIC

# COMMAND ----------

df_anti=df1.join(df2,df1['Dept_Id']==df2['Dept_Id'],'anti')
df_anti.display()
df_anti1=df2.join(df1,df1['Dept_Id']==df2['Dept_Id'],'anti')
df_anti1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW NUMBER()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

#from pyspark.sql import functions as F
##from pyspark.sql.window import Window
#windowSpec=Window.orderBy("Dept_Id")
#df_row=df1.withColumn('RowNum',F.row_number().over(windowSpec))

df_row=df1.withColumn('RowNum',row_number().over(Window.orderBy("Dept_Id")))

df_row.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RANK()

# COMMAND ----------

#from pyspark.sql import functions as F
##from pyspark.sql.window import Window
#windowSpec=Window.orderBy("Dept_Id")
#df_rank=df1.withColumn("Rank",F.rank().over(windowSpec))
df_rank=df1.withColumn("Rank",rank().over(Window.orderBy("Dept_Id")))

df_rank.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DENSE_RANK()

# COMMAND ----------

#from pyspark.sql import functions as F
#from pyspark.sql.window import Window
#windowSpec=Window.orderBy("Dept_Id")
#df_rank1=df_row.withColumn("Rank",F.rank().over(windowSpec))
df_densRank=df1.withColumn("Dense_Rank",dense_rank().over(Window.orderBy("Dept_Id")))

df_densRank.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Row_Number() vs Rank() vs DENSE_RANK()

# COMMAND ----------

df_allRank=df1.withColumn("RowNumber",row_number().over(Window.orderBy("Dept_Id")))\
            .withColumn("Rank",rank().over(Window.orderBy("Dept_Id")))\
            .withColumn("Dense_Rank",dense_rank().over(Window.orderBy("Dept_Id")))\
            .withColumn("Percent_Rank",percent_rank().over(Window.orderBy("Dept_Id")))\
            .withColumn("Cume_Dist",cume_dist().over(Window.orderBy("Dept_Id")))
                
df_allRank.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITE

# COMMAND ----------

df.write.format("csv")\
    .save("/Volumes/workspace/default/samplewrite/sales1/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overwrite Mode

# COMMAND ----------

df.write.format("csv")\
    .mode("overwrite")\
    .save("/Volumes/workspace/default/samplewrite/sales/Sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append Mode

# COMMAND ----------

df.write.format("csv")\
    .mode("append")\
    .save("/Volumes/workspace/default/samplewrite/sales/Sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Error Mode

# COMMAND ----------

df.write.format("csv")\
    .mode("error")\
    .save("/Volumes/workspace/default/samplewrite/sales/Sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ignore Mode

# COMMAND ----------

df.write.format("csv")\
    .mode("ignore")\
    .save("/Volumes/workspace/default/samplewrite/sales/Sales.csv")

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

