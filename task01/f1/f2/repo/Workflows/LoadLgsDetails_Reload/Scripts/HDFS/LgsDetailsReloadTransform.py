from __future__ import print_function
from pyspark.sql import SparkSession
import sys
import argparse
import pyspark.sql.functions as func
import pyspark.sql.functions as sf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col,when,sha2
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from datetime import datetime
from dateutil.relativedelta import *
from pyspark_llap.sql.session import HiveWarehouseSession

spark = SparkSession.builder.appName("WILReload").enableHiveSupport().getOrCreate()
hive = HiveWarehouseSession.session(spark).build()
parser = argparse.ArgumentParser(description='WIL Summary Reload')

spark.conf.set("spark.sql.orc.enabled","true")
spark.conf.set("spark.sql.hive.convertMetastoreOrc","true")
spark.conf.set("spark.sql.orc.char.enabled","true")
spark.conf.set("spark.sql.orc.filterPushdown","true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

parser.add_argument('--database', dest="database", required=True)
parser.add_argument('--source_table', dest="source_table", required=True)
parser.add_argument('--user', dest="user", required=True)
parser.add_argument('--sql', dest="sql", required=True)
parser.add_argument('--output', dest="output", required=True)
parser.add_argument('--lookbacktable', dest="lookback", required=True)
parser.add_argument('--current_date', dest="current_date", required=True)
parser.add_argument('--start_date',dest="start_date",required=True)

args = parser.parse_args()

script_to_run = open(args.sql,"r").read() \
                                     .replace("change_user",args.user) \
                                     .replace("change_database",args.database) \
									 .replace("change_table",args.source_table) \
									 .replace("change_lookback",args.lookback) \
									 .replace("current_date",args.current_date).replace("start_date",args.start_date)


print(script_to_run)
df = hive.executeQuery(script_to_run)

df.write.option("compression", "zlib").mode("append").orc(args.output);