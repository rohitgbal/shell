from __future__ import print_function
from pyspark.sql import SparkSession
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.functions as func
import pyspark.sql.functions as sf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col,when
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from datetime import datetime
from pyspark.sql.functions import col,row_number
from dateutil.relativedelta import *
from pyspark.sql.window import WindowSpec
from pyspark.sql.types import *
from pyspark import since
from pyspark.sql.functions import regexp_replace,col
from pyspark.sql.window import Window
import argparse


#variables
keytab=sys.argv[1]
keytabPrincipal=sys.argv[2]
hive_database_name = sys.argv[3]
output = sys.argv[4]
incremental = sys.argv[5]
source_table = sys.argv[6]
raw_location = sys.argv[7]
input = sys.argv[8]
current_user = sys.argv[9]
json_jar1 = sys.argv[10]
json_jar2 = sys.argv[11]
MKD_schedule_query_days = sys.argv[12]
currentDate = datetime.strptime(sys.argv[13], '%Y-%m-%d')
MKD_schedule_query_day = int(MKD_schedule_query_days)
startDateTime = (currentDate+relativedelta(days=-MKD_schedule_query_day)).strftime('%Y-%m-%d')


print ('Output********', output)
print ('database********', hive_database_name)
print ('source_table********', source_table)
print ('input location********', input)
print ('incremental********', incremental)
print ('MKD_schedule_query_day********', MKD_schedule_query_day)
print ('currentDate********', currentDate)


if __name__ == "__main__":

     spark = SparkSession.builder.appName("MKD Schedule History PJ1").enableHiveSupport().getOrCreate()
     spark.sql("ADD JAR {0}".format(json_jar1))
     spark.sql("ADD JAR {0}".format(json_jar2))



if incremental == "false":
   MKD_history = """Select tblx01_sha_k as sha_key,tblx01_vin_d_3 as vin,tblx01_MKDactivescheduletimestamp_s_3 as MKDactivescheduletimestamp, tblx01_der_event_s_3 as derived_event_timestamp,tblx01_msg_metadata_msg_n as msgmetadata_messagename,tblx01_functionmsgname_x_3 as functionmsgname,tblx01_region_x  as partition_region_x, tblx01_partition_country_x as partition_country_x ,tblx01_partition_date_x as partition_date_x from {0}.ntblx01_CRC4T_msg_pj1_sec_hte WHERE tblx01_vin_d_3 IS NOT NULL AND length(tblx01_vin_d_3)=17 AND tblx01_der_event_s_3 IS NOT NULL AND tblx01_MKDactivescheduletimestamp_s_3 IS NOT NULL AND ( lower(tblx01_functionmsgname_x_3) = (lower('MKDActivationScheduleStatusAlert')) OR lower(tblx01_msg_metadata_msg_n) = (lower('MKDActivationScheduleStatus'))) AND (tblx01_partition_date_x  >= '{1}')""".format(hive_database_name,startDateTime)
   print (MKD_history)
   MKDDailyLoad = spark.sql(MKD_history)

if incremental == "true":
   MKDRaw = """Select sha_key,vin,MKDactivescheduletimestamp,derived_event_timestamp,msgmetadata_messagename,functionmsgname,partition_region_x,partition_country_x,partition_date_x from {0}.nscvs94_temp_tcx_hist_pj1_st_hte WHERE vin IS NOT NULL AND length(vin)=17 AND derived_event_timestamp IS NOT NULL AND MKDactivescheduletimestamp IS NOT NULL AND ( lower(functionmsgname) = (lower('MKDActivationScheduleStatusAlert')) OR lower(msgmetadata_messagename) =(lower('MKDActivationScheduleStatus')))""".format(hive_database_name)
   print (MKDRaw)
   MKDDailyLoad = spark.sql(MKDRaw)


#Load data to Staging Table

MKDFinalData = MKDDailyLoad.select(col("sha_key").alias("sha_k"),
                                   col("vin").alias("vin"),
                                   col("MKDactivescheduletimestamp").alias("MKD_schedule_timestamp"),
		                           col("derived_event_timestamp").alias("event_timestamp"),
		                           col("msgmetadata_messagename").alias("message_name"),
		                           col("functionmsgname").alias("function_msg_name"),
		                           col("partition_region_x").alias("partition_region"),
		                           col("partition_country_x").alias("partition_country"),
		                           col("partition_date_x").alias("partition_date"))

MKDFinalData.repartition(50).write.format("orc").mode("overwrite").save(output)
