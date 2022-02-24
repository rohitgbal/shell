use ${hivevar:db_name};

create table if not exists NSCVPEP_tcx_SETG_SUMM_PII_HTE  
(
   scvpep_vin_d_2 VARCHAR (17),
   scvpep_tcx_st_x_2 String,
   scvpep_strt_s_2 Timestamp,
   scvpep_end_s_2 Timestamp,
   scvpep_rec_stat_x_2 VARCHAR(3),
   scvpep_created_on_s timestamp,
   scvpep_created_by_c varchar(10)
)
partitioned by (scvpep_partition_region_x string, scvpep_partition_country_x string,  scvpep_partition_year_month_x string , scvpep_partition_status_x varchar(3))
clustered by (scvpep_vin_d_2) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvpep_tcx_setg_summ_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');
;

create external table if not exists NSCVSEP_tcx_SETG_SUMM_ST_HTE  
(
   vin VARCHAR (17),
   MKD_state String,
   start_timestamp Timestamp,
   end_timestamp Timestamp,
   record_status VARCHAR(3),
   partition_region string,
   partition_country string,
   partition_year_month string,
   partition_status varchar(3)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvsep_tcx_setg_summ_st_hte' 
TBLPROPERTIES ('orc.compress.size'='8192');
