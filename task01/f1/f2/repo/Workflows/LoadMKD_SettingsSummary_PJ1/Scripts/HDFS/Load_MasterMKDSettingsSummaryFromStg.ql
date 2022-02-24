use ${hivevar:db_name};

set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;


ALTER TABLE NSCVP87_tcx_SETG_PJ1_PII_HTE DROP PARTITION(scvp87_partition_status_x='NEW');

INSERT INTO TABLE NSCVP87_tcx_SETG_PJ1_PII_HTE  PARTITION(scvp87_partition_region_x,scvp87_partition_country_x,scvp87_partition_year_month_x,scvp87_partition_status_x)
SELECT
vin                                   AS       scvp87_vin_d_2
,MKD_state                            AS       scvp87_tcx_st_x_2
,start_timestamp                      AS       scvp87_strt_s_2
,end_timestamp                        AS       scvp87_end_s_2
,record_status                        AS       scvp87_rec_stat_x_2
,FROM_UNIXTIME(UNIX_TIMESTAMP())      AS       scvp87_created_on_s
,"${current_user}"                    AS       scvp87_created_by_c
,partition_region                     AS       scvp87_partition_region_x
,partition_country                    AS       scvp87_partition_country_x
,partition_year_month                 AS       scvp87_partition_year_month_x
,partition_status                     AS       scvp87_partition_status_x 
FROM NSCVS87_tcx_SETG_PJ1_ST_HTE ;