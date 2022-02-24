use ${hivevar:db_name};

--Master Table fields are given same name as the PJ1 source table as per business requirements
--MKD Schedule Master Table
create table if not exists NSCVP95_tcx_SCHED_HIST_PJ1_PII_HTE
(
   tblx01_sha_k varchar(64),
   tblx01_tcx_schedule_sha_k varchar(64),
   tblx01_vin_d_3 Varchar (17),
   tblx01_MKDactivescheduletimestamp_s_3 Timestamp,
   tblx01_der_event_s_3 Timestamp,
   tblx01_msg_metadata_msg_n String,
   tblx01_created_on_s timestamp,
   tblx01_created_by_c varchar(10)
)
partitioned by (tblx01_partition_region_x string, tblx01_partition_country_x string, tblx01_partition_date_x string)
clustered by (tblx01_vin_d_3) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp95_tcx_sched_hist_pj1_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');

--Staging Table
create external table if not exists NSCVS95_tcx_HIST_PJ1_ST_HTE
(
   sha_k varchar(64),
   vin Varchar (17),
   MKD_schedule_timestamp Timestamp,
   event_timestamp Timestamp,
   message_name String,
   function_msg_name String,
   partition_region String,
   partition_country String,
   partition_date String
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvs95_tcx_hist_pj1_st_hte'
TBLPROPERTIES ('orc.compress.size'='8192');

--Staging Temp Table for Inprocess location
create external table if not exists NSCVS94_TEMP_tcx_HIST_PJ1_ST_HTE
(
   sha_key varchar(64),
   vin Varchar (17),
   MKDactivescheduletimestamp Timestamp,
   derived_event_timestamp Timestamp,
   msgmetadata_messagename String,
   functionmsgname String,
   partition_region_x String,
   partition_country_x String,
   partition_date_x String
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/MKD/Inprocess/nscvs94_temp_tcx_hist_pj1_st_hte';

