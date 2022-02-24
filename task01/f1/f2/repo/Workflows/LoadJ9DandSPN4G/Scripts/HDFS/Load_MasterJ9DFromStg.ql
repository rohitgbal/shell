use ${hivevar:db_name};

set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;

alter table NCVDSUM_PACE_CMD_NAME_RSPNS_ST_HTE drop if exists partition(partition_region > '');

msck repair table NCVDSUM_PACE_CMD_NAME_RSPNS_ST_HTE;


INSERT OVERWRITE TABLE NCVDCUM_PACE_CMD_NAME_RSPNS_SEC_HTE PARTITION(cvdcum_partition_region_x,cvdcum_partition_cntry_x,cvdcum_partition_date_x)
SELECT
vin AS cvdcum_vin_d_3
,esn AS cvdcum_esn_d_3
,correlation_id AS cvdcum_crltn_d_3
,app_correlation_id AS cvdcum_app_crltn_d_3
,command_name AS cvdcum_cmd_n_3
,command_timestamp AS cvdcum_cmd_s_3
,command_status AS cvdcum_cmd_stat_x_3
,command_response_timestamp AS cvdcum_cmd_rspns_s_3
,duration AS cvdcum_durn_r_3
,wakeup_sms_flag AS cvdcum_wkup_sms_f_3
,cvrt_version AS cvdcum_cvrt_ver_c_3
,authorization_status AS cvdcum_auth_stat_c_3
,anonymization_flag AS cvdcum_anon_f_3
,cvrt_error_code AS cvdcum_cvrt_err_c_3
,cvrt_error_detail AS cvdcum_cvrt_err_dtl_x_3l
,command_activity_id AS cvdcum_cmd_actvy_d_3
,command_response_activity_id AS cvdcum_cmd_rspns_actvy_d_3
,source AS cvdcum_src_c_3
,tcu_strategy_part_number AS cvdcum_tcu_strat_part_no_c_3
,command_pk AS cvdcum_cmd_pk_x_3
,suspect_flag AS cvdcum_suspct_f_3
,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS cvdcun_created_on_s
,"${current_user}" AS cvdcun_created_by_c
,partition_region AS cvdcum_partition_region_x
,partition_country AS cvdcum_partition_cntry_x
,partition_date AS cvdcum_partition_date_x
FROM NCVDSUM_PACE_CMD_NAME_RSPNS_ST_HTE
WHERE to_date(partition_date) >= date_sub(current_date, ${hivevar:CRC4T_J9D_processdays})