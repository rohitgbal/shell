import sys
import argparse
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("loadWILHistory").enableHiveSupport().getOrCreate()

parser = argparse.ArgumentParser(description='4G WIL Incremental Load')

spark.conf.set("spark.sql.orc.enabled","true")
spark.conf.set("spark.sql.hive.convertMetastoreOrc","true")
spark.conf.set("spark.sql.orc.char.enabled","true")
spark.conf.set("spark.sql.orc.filterPushdown","true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

parser.add_argument('--database', dest="database", required=True)
parser.add_argument('--table', dest="table", required=True)
parser.add_argument('--user', dest="user", required=True)
parser.add_argument('--sql', dest="sql", required=True)
parser.add_argument('--output', dest="output", required=True)
parser.add_argument('--lookbacktable', dest="lookback", required=True)
parser.add_argument('--currdate', dest="curr_date", required=True)

args = parser.parse_args()

script_to_run = open(args.sql,"r").read() \
                                     .replace("change_user",args.user) \
                                     .replace("change_database",args.database) \
									 .replace("change_table",args.table) \
									 .replace("change_lookback",args.lookback) \
									 .replace("curr_date",args.curr_date)

print(script_to_run)
df = spark.sql(script_to_run)

#df.repartition("cvdcvt_partition_region_x","cvdcvt_partition_cntry_x","cvdcvt_partition_date_x").write.option("compression", "zlib") \
#  .mode("overwrite").insertInto(args.output, overwrite=True)

df.write.option("compression", "zlib").mode("append").orc(args.output);