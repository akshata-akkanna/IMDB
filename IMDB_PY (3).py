# Databricks notebook source
#Import Spark Session - Encompasses SparkContext & SQLContext 
from pyspark.sql import SparkSession
#SQL functions
from pyspark.sql import functions
#Get the raw data ----> I have used Databricks Filestore to store my files
sourcepath = "dbfs:/FileStore/tables/tsvfile/title_basics.tsv"
#read the tsv using the spark read function and store it into the dataframe
LoadingTheFile = spark.read.csv(sourcepath,sep = "\t",header = True,inferSchema = True)
#Fetching the past 100 years data by refering to the release date i.e Startyear
FetchingTheYear = LoadingTheFile.where(LoadingTheFile["startYear"] >= 1921)
#Counting movie releases made each year in the past 100 years
FinalDistribution = FetchingTheYear.groupBy('startYear').count().withColumnRenamed('count','NumberOfMovies')
#displaying the result
display(FinalDistribution)



# COMMAND ----------

#Import Spark Session - Encompasses SparkContext & SQLContext 
from pyspark.sql import SparkSession
#SQL functions
from pyspark.sql import functions as fx
#Get the raw data ----> I have used Databricks Filestore to store my files
sourcepath_principle = "dbfs:/FileStore/tables/title_principals.tsv"
sourcepath_Basics = "dbfs:/FileStore/tables/name_basics.tsv"

#read the tsv using the spark read function and store it into the dataframe
LoadingTheFile_principle = spark.read.csv(sourcepath_principle,sep = "\t",header = True,inferSchema = True)
LoadingTheFile_basic = spark.read.csv(sourcepath_Basics,sep = "\t",header = True,inferSchema = True)

#Get the required parameter and value from both title and name dataframe
sel_LoadingTheFile_principle=LoadingTheFile_principle.select(fx.col("tconst"),fx.col("nconst"),fx.col("category"))
sel_LoadingTheFile_basic=LoadingTheFile_basic.select(fx.col("nconst"),fx.col("primaryName"))

#join both the data frame using the nconst
joinedF = sel_LoadingTheFile_principle.join(sel_LoadingTheFile_basic , ["nconst"])

#get only actor and director data from the resltset
join_act_dir = joinedF.where((fx.col("category") == fx.lit("actor")) | (fx.col("category") == fx.lit("director"))) 

#fetching only actor data from the category 
df_with_actor = join_act_dir.withColumn("Actor",(fx.when(fx.col("category") == fx.lit("actor") , fx.col("category")))).where(fx.col("Actor").isNotNull()).withColumnRenamed("primaryName","actorName")

#fetching only director data from the category
df_with_dir = join_act_dir.withColumn("Director",(fx.when(fx.col("category") == fx.lit("director") , fx.col("category")))).where(fx.col("Director").isNotNull()).withColumnRenamed("primaryName","directorName")

#joining all the dataframes using the tconst
joinedADd = join_act_dir.join(df_with_actor,['tconst'] , 'left').join(df_with_dir,['tconst'] , 'left')

#selecting the required parameters 
club_act=joinedADd.where((fx.col("actorName").isNotNull()) & (fx.col("directorName").isNotNull())).select(fx.col("tconst"),fx.col("actorName"),fx.col("directorName"))

#fetching the count of collabration between actor and director using groupBy and aggregate function
finalResult = club_act.groupBy(fx.col("actorName"),fx.col("directorName")).agg(fx.count("directorName").alias("count"))

#Order by high to low 
final =finalResult.orderBy(fx.col("count").desc())

#show top ten collabrations
finalSet = final.head(10)
display(finalSet)

# COMMAND ----------

#Import Spark Session - Encompasses SparkContext & SQLContext 
from pyspark.sql import SparkSession
#SQL functions
from pyspark.sql.functions import split, explode , count
#Get the raw data ----> I have used Databricks Filestore to store my files
sourcepath = "dbfs:/FileStore/tables/tsvfile/title_basics.tsv"
sourcepath_Basics = "dbfs:/FileStore/tables/name_basics.tsv"

#read the tsv using the spark read function and store it into the dataframe
df_title = spark.read.csv(sourcepath,sep = "\t",header = True,inferSchema = True)
df_name = spark.read.csv(sourcepath_Basics,sep = "\t",header = True,inferSchema = True)

#Get the required parameter and value from both title and name dataframe
df_filtered_name = df_name.filter(df_name["primaryName"].isin(['Omar Sy', 'Saoirse Ronan',  'Frances McDormand'])).select('primaryName','knownForTitles')
df_filtered_title = df_title.select('tconst','titleType','genres')

#As the data in the KnowForTitle is in the form of array lets split the data using comma seperater and remane the column
splitTitles = df_filtered_name.withColumn("knownForTitles", explode(split(col("knownForTitles"), ","))).withColumnRenamed("knownForTitles","tconst")

#Joining splited dataframe and title dataframe using tconst 
join_both = df_filtered_title.join(splitTitles,["tconst"])

#As the genes data is in the array format split the data
genes = join_both.withColumn("genres", explode(split(col("genres"), ",")))

#Getting the count of each genres acted by repspective Actor
finalResulr = genes.groupBy(("genres"),("primaryName")).agg(count("genres").alias("NumberofGenres"))

#Displaying the final result
display(finalResulr)
