use ${hivevar:db_name};

set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;

alter table NSCVS92_J9D_PJ1_ST_HTE drop if exists partition(partition_region > '');

msck repair table NSCVS92_J9D_PJ1_ST_HTE;


INSERT OVERWRITE TABLE NSCVC92_J9D_PJ1_SEC_HTE PARTITION(scvc92_partition_region_x,scvc92_partition_cntry_x,scvc92_partition_date_x)
SELECT
vin AS scvc92_vin_d_3
,esn AS scvc92_esn_d_3
,correlation_id AS scvc92_crltn_d_3
,app_correlation_id AS scvc92_app_crltn_d_3
,command_name AS scvc92_cmd_n_3
,command_timestamp AS scvc92_cmd_s_3
,command_status AS scvc92_cmd_stat_x_3
,command_response_timestamp AS scvc92_cmd_rspns_s_3
,duration AS scvc92_durn_r_3
,wakeup_sms_flag AS scvc92_wkup_sms_f_3
,cvrt_version AS scvc92_cvrt_ver_c_3
,authorization_status AS scvc92_auth_stat_c_3
,anonymization_flag AS scvc92_anon_f_3
,cvrt_error_code AS scvc92_cvrt_err_c_3
,cvrt_error_detail AS scvc92_cvrt_err_dtl_x_3l
,command_activity_id AS scvc92_cmd_actvy_d_3
,command_response_activity_id AS scvc92_cmd_rspns_actvy_d_3
,source AS scvc92_src_c_3
,tcu_strategy_part_number AS scvc92_tcu_strat_part_no_c_3
,command_pk AS scvc92_cmd_pk_x_3
,suspect_flag AS scvc92_suspct_f_3
,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS scvc92_created_on_s
,"${current_user}" AS scvc92_created_by_c
,partition_region AS scvc92_partition_region_x
,partition_country AS scvc92_partition_cntry_x
,partition_date AS scvc92_partition_date_x
FROM NSCVS92_J9D_PJ1_ST_HTE
WHERE to_date(partition_date) >= date_sub(current_date, ${hivevar:CRC4T_J9D_pj1_processdays});