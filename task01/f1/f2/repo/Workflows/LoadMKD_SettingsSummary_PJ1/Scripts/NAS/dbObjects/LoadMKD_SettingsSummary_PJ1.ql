use ${hivevar:db_name};

create table if not exists NSCVP87_tcx_SETG_PJ1_PII_HTE  
(
   scvp87_vin_d_2 VARCHAR (17),
   scvp87_tcx_st_x_2 String,
   scvp87_strt_s_2 Timestamp,
   scvp87_end_s_2 Timestamp,
   scvp87_rec_stat_x_2 VARCHAR(3),
   scvp87_created_on_s timestamp,
   scvp87_created_by_c varchar(10)
)
partitioned by (scvp87_partition_region_x string, scvp87_partition_country_x string,  scvp87_partition_year_month_x string , scvp87_partition_status_x varchar(3))
clustered by (scvp87_vin_d_2) into 10 buckets
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Secure/dataprogRepository/nscvp87_tcx_setg_pj1_pii_hte'
tblproperties ('compactorthreshold.hive.compactor.delta.num.threshold'='1','compactorthreshold.hive.compactor.delta.pct.threshold'='0.5','transactional'='true','orc.compress.size'='8192');
;

create external table if not exists NSCVS87_tcx_SETG_PJ1_ST_HTE  
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
LOCATION '/project/prg1/prg1${hivevar:hdfs_env}/Warehouse/Staging/dataprogRepository/nscvs87_tcx_setg_pj1_st_hte' 
TBLPROPERTIES ('orc.compress.size'='8192');

