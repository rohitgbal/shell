use ${hivevar:db_name};

set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;

alter table NCVDCUL_PACE_TCU_STRAT_PART_NO_SEC_HTE drop if exists partition(cvdcul_partition_country_x >= '');

msck repair table NCVDCUL_PACE_TCU_STRAT_PART_NO_SEC_HTE;


INSERT INTO TABLE NCVDCUL_PACE_TCU_STRAT_PART_NO_SEC_HTE PARTITION(cvdcul_partition_country_x)
SELECT    
cvdcul_vin_d_3 ,
cvdcul_tcu_strat_part_no_c_3, 
cvdcul_cvrt_ver_c_3 ,
cvdcul_first_assoc_s_3, 
cvdcul_last_assoc_s_3 ,
cvdcul_event_s_3 ,
cvdcul_latest_msg_n_3, 
cvdcul_created_on_s  ,
cvdcul_created_by_c ,
cvdcul_partition_country_x
FROM NCVDSUL_PACE_TCU_STRAT_PART_NO_ST_HTE;