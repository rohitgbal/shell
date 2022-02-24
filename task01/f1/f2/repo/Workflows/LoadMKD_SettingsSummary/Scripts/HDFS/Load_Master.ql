use ${hivevar:db_name};
set hive.execution.engine=tez;
set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000000;
set hive.exec.parallel=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.stats.autogather=false;
set tez.am.resource.memory.mb=16384;
set hive.tez.container.size=32768;
set hive.exec.orc.split.strategy=BI;

ALTER TABLE ${master_table} DROP PARTITION(scvpep_partition_status_x='NEW');

INSERT INTO TABLE ${master_table}  PARTITION(scvpep_partition_region_x,scvpep_partition_country_x,scvpep_partition_year_month_x,scvpep_partition_status_x)
SELECT
vin                                   AS       scvpep_vin_d_2
,MKD_state                            AS       scvpep_tcx_st_x_2
,start_timestamp                      AS       scvpep_strt_s_2
,end_timestamp                        AS       scvpep_end_s_2
,record_status                        AS       scvpep_rec_stat_x_2
,FROM_UNIXTIME(UNIX_TIMESTAMP())      AS       scvpep_created_on_s
,"${current_user}"                    AS       scvpep_created_by_c
,partition_region                     AS       scvpep_partition_region_x
,partition_country                    AS       scvpep_partition_country_x
,partition_year_month                 AS       scvpep_partition_year_month_x
,partition_status                     AS       scvpep_partition_status_x 
FROM ${staging_table} ;