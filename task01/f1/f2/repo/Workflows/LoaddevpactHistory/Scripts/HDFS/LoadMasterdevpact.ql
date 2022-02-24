use ${hivevar:db_name};

set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;


INSERT INTO TABLE NSCVPFY_tcx_SCHED_HIST_PII_HTE  PARTITION(scvpfy_partition_region_x,scvpfy_partition_country_x,scvpfy_partition_date_x)
Select sha_k as scvpfy_sha_k,
(reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',CONCAT(COALESCE(vin,""),
COALESCE(MKD_schedule_timestamp,""),COALESCE(event_timestamp,""),COALESCE(message_name,"")
,COALESCE(partition_region,""),COALESCE(partition_country,""),COALESCE(partition_date,"")))) as scvpfy_tcx_schedule_sha_k,
vin as scvpfy_vin_d_3,MKD_schedule_timestamp as scvpfy_MKDactivescheduletimestamp_s_3,event_timestamp as scvpfy_der_event_s_3,message_name as scvpfy_msg_metadata_msg_n,FROM_UNIXTIME(UNIX_TIMESTAMP()) as scvpfy_created_on_s,
'${hivevar:current_user}' as scvpfy_created_by_c,
partition_region as scvpfy_partition_region_x,partition_country as scvpfy_partition_country_x,
partition_date as scvpfy_partition_date_x
From NSCVSFY_tcx_HIST_ST_HTE Left Join NSCVPFY_tcx_SCHED_HIST_PII_HTE Master ON
(reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',CONCAT(COALESCE(vin,""),
COALESCE(MKD_schedule_timestamp,""),COALESCE(event_timestamp,""),COALESCE(message_name,"")
,COALESCE(partition_region,""),COALESCE(partition_country,""),COALESCE(partition_date,"")))) = Master.scvpfy_tcx_schedule_sha_k
where
Master.scvpfy_tcx_schedule_sha_k is null;