from __future__ import print_function
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.functions as sf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col,when
from pyspark_llap.sql.session import HiveWarehouseSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.storagelevel import StorageLevel
from dateutil.relativedelta import *
import argparse
import sys

spark = SparkSession.builder.appName("loadH2TDetails").enableHiveSupport().getOrCreate()
##hive = HiveWarehouseSession.session(spark).build()
parser = argparse.ArgumentParser(description='4G H2T Incremental Load')

spark.conf.set("hive.exec.dynamic.partition","true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
spark.conf.set("hive.exec.max.dynamic.partitions.pernode","1000000")
spark.conf.set("hive.execution.engine","tez")
spark.conf.set("hive.exec.orc.split.strategy","BI")
spark.conf.set("hive.exec.parallel","true")
spark.conf.set("hive.tez.auto.reducer.parallelism","true")
spark.conf.set("hive.stats.autogather","false")
spark.conf.set("tez.am.resource.memory.mb","32768")
spark.conf.set("hive.tez.container.size","32768")
spark.conf.set("hive.optimize.sort.dynamic.partition","false")
spark.conf.set("hive.exec.reducers.bytes.per.reducer","25769790581")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

parser.add_argument('--database', dest="database", required=True)
parser.add_argument('--output', dest="output", required=True)
parser.add_argument('--user', dest="user", required=True)

parser.add_argument('--currdate', dest="currdate", required=True)
parser.add_argument('--lkbdays', dest="lkbdays", required=True)
parser.add_argument('--historyload', dest="historyload", required=True)
parser.add_argument('--tempstgtable', dest="tempstgtable", required=True)

args = parser.parse_args()

select_temp= spark.sql("""select * from {0}.{1}""".format(args.database,args.tempstgtable))

##select_temp.persist(StorageLevel.MEMORY_AND_DISK)

rowIdWdw=Window.partitionBy("VIN")

cvrtWdw = Window.partitionBy("VIN","entity_id","entity_type","consent_status")
cnstWdw = Window.partitionBy("VIN").orderBy("consent_start_time")
pteWdw = Window.partitionBy("VIN","entity_id","entity_type")
labsWdw = row_number().over((pteWdw).orderBy("consent_start_time","row_id"))
rabsWdw = row_number().over((cvrtWdw).orderBy("consent_start_time","row_id"))
absolWdw = abs(labsWdw - rabsWdw)
scvrtWdw = Window.partitionBy("VIN",absolWdw)
unbndWdw = scvrtWdw.orderBy("consent_start_time").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
pTWdw = Window.partitionBy("vin","entity_id","entity_type","consent_status",absolWdw)


select_post_temp=select_temp.withColumn("row_id",row_number().over((rowIdWdw).orderBy("consent_start_time","consent_status","consent_sequence_id"))).select("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","consent_sequence_id","consent_start_time","pte_version","pte_timestamp","ufm_version","ufm_timestamp","partition_region","partition_country","row_id")

select_pre_temp1=select_post_temp.withColumn("start_entity_no",row_number().over(pteWdw.orderBy("consent_start_time","row_id"))) \
.withColumn("start_setting_no",row_number().over(cvrtWdw.orderBy("consent_start_time","row_id"))) \
.select("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","consent_sequence_id","consent_start_time","pte_version","pte_timestamp","ufm_version","ufm_timestamp","partition_region","partition_country","start_entity_no","start_setting_no","row_id")



select_temp1=select_pre_temp1.withColumn("cvrt_version",last("cvrt_version",True).over(unbndWdw)) \
.withColumn("iccid",last("iccid",True).over(unbndWdw)) \
.withColumn("esn",last("esn",True).over(unbndWdw)) \
.withColumn("pte_version",first("pte_version",True).over(pTWdw.orderBy("pte_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("pte_timestamp",first("pte_timestamp",True).over(pTWdw.orderBy("pte_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("ufm_version",first("ufm_version",True).over(pTWdw.orderBy("ufm_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("ufm_timestamp",first("ufm_timestamp",True).over(pTWdw.orderBy("ufm_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("partition_region",last("partition_region",True).over(unbndWdw)) \
.withColumn("partition_country",last("partition_country",True).over(unbndWdw)) \
.select("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","consent_sequence_id","consent_start_time","pte_version","pte_timestamp","ufm_version","ufm_timestamp","start_entity_no","start_setting_no","partition_region","partition_country").distinct()


select_temp2=select_temp1.groupBy("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","pte_version","pte_timestamp","ufm_version","ufm_timestamp","partition_region","partition_country",abs(col('start_entity_no')-col('start_setting_no'))).agg(min("consent_start_time").alias("consent_start_time"),max("consent_start_time").alias("last_start_time"),collect_set("consent_sequence_id").alias("consent_sequence_id"))


print('select_temp2>>',select_temp2)

select_temp2.createOrReplaceTempView("StagingFinal")


loadstaging=spark.sql("""select st.vin,st.cvrt_version,st.iccid,st.esn,st.entity_id,st.entity_type,st.consent_status,st.consent_sequence_id,st.consent_start_time,coalesce(lead(st.consent_start_time, 1) over (partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time asc), last_start_time) as consent_end_time,st.pte_version,st.pte_timestamp,st.ufm_version,st.ufm_timestamp,case when row_number() over (partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time desc) = 1 then 'NEW' else 'OLD' end as record_status,'{0}' as created_by_c,from_unixtime(unix_timestamp()) as created_on_s,st.partition_region,st.partition_country,date_format(st.consent_start_time,'yyyy-MM') as partition_date,case when row_number() over(partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time desc) = 1 then 'NEW' when datediff('{1}', to_date(coalesce(lead(st.consent_start_time, 1) over (partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time asc), last_start_time))) <= {2} then 'NEW' else 'OLD' end as partition_status from StagingFinal st""".format(args.user,args.currdate,args.lkbdays))

        	
#df.repartition("cvdcvt_partition_region_x","cvdcvt_partition_cntry_x","cvdcvt_partition_date_x").write.option("compression", "zlib") \
#  .mode("overwrite").insertInto(args.output, overwrite=True)

loadstaging.write.option("compression", "zlib").mode("append").orc(args.output)