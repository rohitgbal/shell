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



spark = SparkSession.builder.appName("loadH2TDetailsPJ1").enableHiveSupport().getOrCreate()
hive = HiveWarehouseSession.session(spark).build()
parser = argparse.ArgumentParser(description='4G H2T PJ1 Incremental Load')

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
parser.add_argument('--rawtable', dest="rawtable", required=True)
parser.add_argument('--J9Dtable', dest="J9Dtable", required=True)
parser.add_argument('--H2Ttable', dest="H2Ttable", required=True)
parser.add_argument('--user', dest="user", required=True)
parser.add_argument('--output', dest="output", required=True)
parser.add_argument('--lookbacktable', dest="lookback", required=True)
parser.add_argument('--currdate', dest="curr_date", required=True)



args = parser.parse_args()

select_un1_temp = spark.sql("""select tblx01_vin_d_3 as vin, tblx01_pro_to_file_ver_x as cvrt_version,tblx01_icc_d_2 as iccid,tblx01_esn_r as esn,sttg.entityId as entity_id,sttg.entityType as entity_type,sttg.optIn as consent_status,sttg.consentSequenceId as consent_sequence_id,sttg.consenttimestamp as consent_start_time,case  when isnull(tblx01_policytableextensiontimestamp_s_3) then null else  concat(tblx01_policytableextension_platformversion_r_3,".",tblx01_policytableextension_majorversion_r_3,".",tblx01_policytableextension_minorversion_r_3) end as pte_version,tblx01_policytableextensiontimestamp_s_3 as pte_timestamp,case when isnull(tblx01_userfriendlymessagestimestamp_s_3) then null else  concat(tblx01_userfriendlymessages_platformversion_r_3,".",tblx01_userfriendlymessages_majorversion_r_3,".",tblx01_userfriendlymessages_minorversion_r_3)end as ufm_version,tblx01_userfriendlymessagestimestamp_s_3 as ufm_timestamp,case when isnull(tblx01_H2Tserviceconfigfiletimestamp_s_3) then null else concat(tblx01_H2Tserviceconfigfile_platformversion_r_3,".",tblx01_H2Tserviceconfigfile_majorversion_r_3,".",tblx01_H2Tserviceconfigfile_minorversion_r_3) end as scf_version,tblx01_H2Tserviceconfigfiletimestamp_s_3 as scf_timestamp,tblx01_region_x as partition_region,tblx01_partition_country_x as partition_country from {0}.{1} lateral view explode(tblx01_entity_settings_x_3) es as sttg where lower(tblx01_msg_metadata_msg_typ_x) in (lower('Alert'), lower('QueryResponse')) and lower(tblx01_msg_metadata_msg_n) like lower('%H2T%') and tblx01_partition_date_x >= date_sub('{2}',{3}) and tblx01_partition_date_x < '{2}' and to_date(sttg.consenttimestamp)>= date_sub('{2}',{3}) and to_date(sttg.consenttimestamp)< '{2}' and tblx01_vin_d_3 is not null  and length(tblx01_vin_d_3) == 17 and sttg.entityId is not null and tblx01_msg_metadata_msg_typ_x is not null and sttg.optIn is not null and sttg.consenttimestamp is not null""".format(args.database,args.rawtable,args.curr_date,args.lookback))

select_un2_temp = spark.sql("""select a.vin,a.cvrt_version,a.iccid,a.esn,a.entity_id,a.entity_type,a.consent_status,a.consent_sequence_id,a.consent_start_time,a.pte_version,a.pte_timestamp,a.ufm_version,a.ufm_timestamp,a.scf_version,a.scf_timestamp,a.partition_region,a.partition_country from(select tblx01_vin_d_3 as vin ,tblx01_pro_to_file_ver_x as cvrt_version,tblx01_cloud_msg_d,tblx01_msg_metadata_msg_n,tblx01_app_crltn_d_3,tblx01_icc_d_2 as iccid,tblx01_esn_r as esn,sttg.entityId as entity_id,sttg.entityType as entity_type,sttg.optIn as consent_status,sttg.consentSequenceId as consent_sequence_id,sttg.ConsentTimestamp as consent_start_time,case when isnull(tblx01_policytableextensiontimestamp_s_3) then null else concat(tblx01_policytableextension_platformversion_r_3,".",tblx01_policytableextension_majorversion_r_3,".",tblx01_policytableextension_minorversion_r_3)end as pte_version,tblx01_policytableextensiontimestamp_s_3 as pte_timestamp,case when isnull(tblx01_userfriendlymessagestimestamp_s_3) then null else concat(tblx01_userfriendlymessages_platformversion_r_3,".",tblx01_userfriendlymessages_majorversion_r_3,".",tblx01_userfriendlymessages_minorversion_r_3)end as ufm_version,tblx01_userfriendlymessagestimestamp_s_3 as ufm_timestamp,case when isnull(tblx01_H2Tserviceconfigfiletimestamp_s_3) then null else concat(tblx01_H2Tserviceconfigfile_platformversion_r_3,".",tblx01_H2Tserviceconfigfile_majorversion_r_3,".",tblx01_H2Tserviceconfigfile_minorversion_r_3) end as scf_version,tblx01_H2Tserviceconfigfiletimestamp_s_3 as scf_timestamp,tblx01_region_x as partition_region,tblx01_partition_country_x as partition_country from {0}.{1} lateral view explode(tblx01_entity_settings_x_3) es as sttg where lower(tblx01_msg_metadata_msg_typ_x) = lower('Command') and lower(tblx01_msg_metadata_msg_n) like (lower('%H2T%')) and tblx01_partition_date_x >= date_sub('{3}',{4}) and tblx01_partition_date_x < '{3}' and to_date(sttg.consenttimestamp)>= date_sub('{3}',{4}) and to_date(sttg.consenttimestamp)< '{3}' and tblx01_vin_d_3 is not null and length(tblx01_vin_d_3) == 17 and sttg.entityId is not null and tblx01_msg_metadata_msg_typ_x is not null and sttg.optIn is not null and sttg.consenttimestamp is not null) a join {0}.{2} b ON a.vin = b.scvc92_vin_d_3 and (a.tblx01_cloud_msg_d = b.scvc92_crltn_d_3 or a.tblx01_app_crltn_d_3 = b.scvc92_app_crltn_d_3) and a.tblx01_msg_metadata_msg_n = b.scvc92_cmd_n_3 and lower(b.scvc92_cmd_stat_x_3) = lower('SUCCESS') and a.tblx01_cloud_msg_d is not null and a.tblx01_app_crltn_d_3 is not null and b.scvc92_cmd_s_3 is not null and b.scvc92_cmd_rspns_s_3 is not null""".format(args.database,args.rawtable,args.J9Dtable,args.curr_date,args.lookback))

select_un3_temp = hive.executeQuery("""select scvpbb_vin_d_2,scvpbb_cvrt_ver_x_2,scvpbb_icc_d_2,scvpbb_esn_x_2,scvpbb_enty_d_2,scvpbb_enty_typ_x_2,scvpbb_consent_stat_x_2,scvpbb_consent_seq_id_2 as scvpbb_consent_seq_d_2,scvpbb_consent_strt_s_2,scvpbb_pte_ver_x_2, scvpbb_pte_s_2, scvpbb_ufm_ver_x_2,scvpbb_ufm_s_2,scvpbb_scf_ver_x_2,scvpbb_scf_s_2, scvpbb_partition_region_x,scvpbb_partition_cntry_x from {0}.{1} lateral view explode(scvpbb_consent_seq_d_2) seqTable as scvpbb_consent_seq_id_2 where scvpbb_partition_status_x = 'NEW'""".format(args.database,args.H2Ttable))
 
select_un4_temp = hive.executeQuery("""select scvpbb_vin_d_2,scvpbb_cvrt_ver_x_2,scvpbb_icc_d_2,scvpbb_esn_x_2,scvpbb_enty_d_2,scvpbb_enty_typ_x_2,scvpbb_consent_stat_x_2,scvpbb_consent_seq_id_2 as scvpbb_consent_seq_d_2,scvpbb_consent_end_s_2 as scvpbb_consent_strt_s_2,scvpbb_pte_ver_x_2,scvpbb_pte_s_2,scvpbb_ufm_ver_x_2,scvpbb_ufm_s_2,scvpbb_scf_ver_x_2,scvpbb_scf_s_2,scvpbb_partition_region_x,scvpbb_partition_cntry_x from {0}.{1} lateral view explode(scvpbb_consent_seq_d_2) seqTable as scvpbb_consent_seq_id_2 where scvpbb_partition_status_x = 'NEW' and scvpbb_rec_stat_x_2 = 'NEW'and scvpbb_consent_strt_s_2 != scvpbb_consent_end_s_2""".format(args.database,args.H2Ttable))


select_temp= select_un1_temp.union(select_un2_temp).union(select_un3_temp).union(select_un4_temp) 

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


select_post_temp=select_temp.withColumn("row_id",row_number().over((rowIdWdw).orderBy("consent_start_time","consent_status","consent_sequence_id"))).select("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","consent_sequence_id","consent_start_time","pte_version","pte_timestamp","ufm_version","ufm_timestamp","scf_version","scf_timestamp","partition_region","partition_country","row_id")

select_pre_temp1=select_post_temp.withColumn("start_entity_no",row_number().over(pteWdw.orderBy("consent_start_time","row_id"))) \
.withColumn("start_setting_no",row_number().over(cvrtWdw.orderBy("consent_start_time","row_id"))) \
.select("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","consent_sequence_id","consent_start_time","pte_version","pte_timestamp","ufm_version","ufm_timestamp","scf_version","scf_timestamp","partition_region","partition_country","start_entity_no","start_setting_no","row_id")





select_temp1=select_pre_temp1.withColumn("cvrt_version",last("cvrt_version",True).over(unbndWdw)) \
.withColumn("iccid",last("iccid",True).over(unbndWdw)) \
.withColumn("esn",last("esn",True).over(unbndWdw)) \
.withColumn("pte_version",first("pte_version",True).over(pTWdw.orderBy("pte_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("pte_timestamp",first("pte_timestamp",True).over(pTWdw.orderBy("pte_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("ufm_version",first("ufm_version",True).over(pTWdw.orderBy("ufm_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("ufm_timestamp",first("ufm_timestamp",True).over(pTWdw.orderBy("ufm_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("scf_version",first("scf_version",True).over(pTWdw.orderBy("scf_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("scf_timestamp",first("scf_timestamp",True).over(pTWdw.orderBy("scf_timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
.withColumn("partition_region",last("partition_region",True).over(unbndWdw)) \
.withColumn("partition_country",last("partition_country",True).over(unbndWdw)) \
.select("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","consent_sequence_id","consent_start_time","pte_version","pte_timestamp","ufm_version","ufm_timestamp","scf_version","scf_timestamp","start_entity_no","start_setting_no","partition_region","partition_country").distinct()

        	
select_temp2=select_temp1.groupBy("vin", "cvrt_version", "iccid","esn","entity_id","entity_type","consent_status","pte_version","pte_timestamp","ufm_version","ufm_timestamp","scf_version","scf_timestamp","partition_region","partition_country",abs(col('start_entity_no')-col('start_setting_no'))).agg(min("consent_start_time").alias("consent_start_time"),max("consent_start_time").alias("last_start_time"),collect_set("consent_sequence_id").alias("consent_sequence_id"))

select_temp2.createOrReplaceTempView("StagingFinal")     	

loadstaging=spark.sql("""select st.vin,st.cvrt_version,st.iccid,st.esn,st.entity_id,st.entity_type,st.consent_status,st.consent_sequence_id,st.consent_start_time,coalesce(lead(st.consent_start_time, 1) over (partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time asc), last_start_time) as consent_end_time,st.pte_version,st.pte_timestamp,st.ufm_version,st.ufm_timestamp,st.scf_version,st.scf_timestamp,case when row_number() over (partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time desc) = 1 then 'NEW' else 'OLD' end as record_status,from_unixtime(unix_timestamp()) as created_on_s,'{0}' as created_by_c,st.partition_region,st.partition_country,date_format(st.consent_start_time,'yyyy-MM') as partition_date,case when row_number() over(partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time desc) = 1 then 'NEW' when datediff('{1}', to_date(coalesce(lead(st.consent_start_time, 1) over (partition by st.vin, st.entity_id, st.entity_type order by st.consent_start_time asc), last_start_time))) <= 30 then 'NEW' else 'OLD' end as partition_status from StagingFinal st""".format(args.user,args.curr_date))
        	
#print ("loadstaging")
#loadstaging.show()
        	
#df.repartition("cvdcvt_partition_region_x","cvdcvt_partition_cntry_x","cvdcvt_partition_date_x").write.option("compression", "zlib") \
#  .mode("overwrite").insertInto(args.output, overwrite=True)

loadstaging.write.option("compression", "zlib").mode("append").orc(args.output)