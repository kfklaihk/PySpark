
import pyspark
import os
import logging


from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import LongType , StringType


logger = logging.getLogger()
logger.setLevel('INFO')
BASIC_FORMAT = "%(asctime)s:%(levelname)s:%(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
chlr = logging.StreamHandler()
chlr.setFormatter(formatter)
fhlr = logging.FileHandler('KaiOS_test_run.log') 
fhlr.setFormatter(formatter)
logger.addHandler(chlr)
logger.addHandler(fhlr)
logger.info('KaiOS PySpark Test Run Starts')
logger.info('Make Sure the last.fm data file is in the working directory')
logger.info('The Test outputs and logs are generated in the working directory')

spark = SparkSession.builder.appName("KOS_Test").getOrCreate()
curdir = (os.getcwd()).replace("\\", "\\\\")
data_timestamp_csv = curdir + "\\"+"userid-timestamp-artid-artname-traid-traname.tsv"
#data_profile_csv = curdir + "userid-profile.tsv"
logger.info('Reading Data File to DataFrame')
data_timestamp = spark.read.option("delimiter","\t").option("header","false").csv(data_timestamp_csv).toDF("userid","timestamp","artistid","artistname","trackid","trackname")
#data_profile = spark.read.option("delimiter","\t").option("header","true").csv(data_profile_csv).toDF("userid","gender","age","country","registereddate")
logger.info('Start Test Q1')
logger.info('ReFormat TimeStamp')
data_timestamp=data_timestamp.withColumn("replaced_timestamp", regexp_replace(col("timestamp"), "[TZ]", " ")).withColumn("formatted_timestamp",to_timestamp(col("replaced_timestamp"),"yyyy-MM-dd HH:mm:ss ")).drop(col("timestamp")).drop(col("replaced_timestamp")).sort("userid","formatted_timestamp")
data_timestamp_1=data_timestamp.withColumn("row_no", func.row_number().over(Window.partitionBy(col("userid")).orderBy(col("formatted_timestamp"))))
logger.info('Calculate duration differences between songs played by a user')
data_timestamp_2=data_timestamp_1.withColumn("lead_timestamp", func.lead(col("formatted_timestamp")).over(Window.partitionBy(col("userid")).orderBy(col("formatted_timestamp")))).filter("lead_timestamp is Not Null").withColumn("DiffInMinutes",round((col("lead_timestamp").cast(LongType())- col("formatted_timestamp").cast(LongType()))/60))
logger.info('Group the songs to sessions')
data_timestamp_3=data_timestamp_2.withColumn("session_group", func.sum(when((col("DiffInMinutes")>20),1).otherwise(0)).over(Window.partitionBy("userid").orderBy("row_no").rangeBetween(Window.unboundedPreceding, 0))).withColumn("session_group_id",concat(col("userid"),lit("_"),col("session_group")))
data_timestamp_grp=data_timestamp_3.filter("DiffInMinutes <21").groupby("session_group_id")
logger.info('Find Top 10 longest sessions')
data_timestamp_grp_1=data_timestamp_grp.agg(sum("DiffInMinutes")).toDF('session_group_id','session_minutes').sort('session_minutes', ascending=False).withColumn("row_num", func.row_number().over(Window.orderBy(col("session_minutes").desc()))).filter("row_num < 11")
data_timestamp_3_subset=data_timestamp_3.filter("DiffInMinutes <21")  
logger.info('Format the desired outputs')
data_timestamp_grp_2=data_timestamp_grp_1.join(data_timestamp_3_subset, ["session_group_id"]).withColumn("first_timestamp", func.first(col("formatted_timestamp")).over(Window.partitionBy(col("session_group_id")).orderBy(col("formatted_timestamp")))).withColumn("last_timestamp", func.last(col("formatted_timestamp")).over(Window.partitionBy(col("session_group_id")).orderBy(col("formatted_timestamp")).rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).select(['userid',"trackname","first_timestamp","last_timestamp"])
logger.info('Write Q1 outputs to csv')
data_timestamp_grp_2.write.csv('Test_Q1.csv',header=True)





logger.info('Start Test Q2')
data_timestamp_grp_song=data_timestamp.groupby("trackid")
logger.info('Get Top 100 most popular songs identified by trackid')
data_timestamp_grp_song_2=data_timestamp_grp_song.agg(count("trackid")).toDF('trackid','times_played').sort('times_played', ascending=False).withColumn("row_num", func.row_number().over(Window.orderBy(col("times_played").desc()))).filter("row_num < 101")
logger.info('Format the desired outputs')
data_timestamp_grp_song_3=data_timestamp_grp_song_2.join(data_timestamp.select(['trackid',"artistname","trackname"]).distinct(), ['trackid']).select(['trackid','times_played',"artistname","trackname"])
logger.info('Write Q2 outputs to csv')
data_timestamp_grp_song_3.write.csv('Test_Q2.csv',header=True)


logger.info('Start Test Q3')
data_timestamp_grp_user=data_timestamp.groupby("userid")
logger.info('Get list of songs identified by trackid for each user')
data_timestamp_grp_user_2=data_timestamp_grp_user.agg(countDistinct('trackid')).toDF('userid','num_songs_played')
logger.info('Write Q3 outputs to csv')
data_timestamp_grp_user_2.write.csv('Test_Q3.csv',header=True)
logger.info('End KaiOS Test')


