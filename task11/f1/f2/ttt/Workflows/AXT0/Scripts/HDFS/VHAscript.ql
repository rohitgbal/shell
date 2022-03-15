use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=BI;
set hive.orc.cache.use.soft.references=true;
set hive.tez.java.opts=-Xmx24576m;
set hive.tez.container.size=32768;
set tez.am.resource.memory.mb=16384;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;


insert into ncvdcvt_AXT_summary_sec_acid_hte
partition(cvdcvt_partition_cntry_x,cvdcvt_partition_date_x)
select
sha_key as cvdcvt_sha_k,
sha2(CONCAT(COALESCE(vin,""),COALESCE(AXT_id,""),COALESCE(ecu_id,""),COALESCE(cast(event_time as string),"")),256) as cvdsvt_sha_k_AXT,
vin as cvdcvt_vin_d_3,
event_time as cvdcvt_event_s_3,
number as cvdcvt_nump_r_2,
longitude as cvdcvt_lon_r_3,
latitude cvdcvt_lat_r_3,
ecu_id as cvdcvt_ecu_d_2,
AXT_id as cvdcvt_AXT_d_2,
AXT_info_byte as cvdcvt_AXT_info_byte_x_2 ,
AXT_status_byte as cvdcvt_AXT_status_byte_x_2,
test_failed as cvdcvt_test_fald_r_2,
test_failed_this_operation_cycle as cvdcvt_test_fald_this_oper_cyc_r_2,
pending_AXT as cvdcvt_pend_AXT_r_2,
confirmed_AXT as cvdcvt_confmd_AXT_r_2,
test_not_completed_since_last_clear as cvdcvt_test_not_cmpltd_since_lst_clr_r_2,
test_failed_since_last_clear as cvdcvt_test_fald_since_lst_clr_r_2,
test_not_completed_this_operation_cycle as cvdcvt_test_not_cmpltd_this_oper_cyc_r_2,
warning_indicator_requested as cvdcvt_wrng_ind_reqstd_r_2,
source as cvdcvt_src_c,
partition_region as cvdcvt_region_x,
'prg1PROD' as cvdcvt_created_by_c,
from_unixtime(unix_timestamp()) as cvdcvt_created_on_s,
raw_payload_metadata_cvrt_ver as cvdcvt_raw_payload_metadata_cvrt_ver_x,
auth_stat as cvdcvt_auth_stat_c,
'Normal' as cvdcvt_lifecycmde_d_actl_r,
partition_country as cvdcvt_partition_cntry_x,
partition_date as cvdcvt_partition_date_x
from
(select
cvdc60_sha_k as sha_key,
cvdc60_vd_vin_r_2 as vin,
IF(cvdc60_calcd_data_gathered_s_2 > cvdc60_ap_calcd_message_datetime_s_2,cvdc60_ap_calcd_message_datetime_s_2,cvdc60_calcd_data_gathered_s_2) as event_time,
cvdc60_vd_number_uds_metric_r_2 as number,
CONCAT('0x', SUBSTR(UPPER(key.ecuid),-3,3)) AS ecu_id,
CASE WHEN LENGTH(UPPER(AXT.AXTcode)) = 4 THEN CONCAT(SUBSTR(UPPER(AXT.AXTcode),1,1),'0',SUBSTR(UPPER(AXT.AXTcode),2,3))
WHEN LENGTH(UPPER(AXT.AXTcode)) = 3 THEN CONCAT(SUBSTR(UPPER(AXT.AXTcode),1,1),'00',SUBSTR(UPPER(AXT.AXTcode),2,2))
WHEN LENGTH(UPPER(AXT.AXTcode)) = 2 THEN CONCAT(SUBSTR(UPPER(AXT.AXTcode),1,1),'000',SUBSTR(UPPER(AXT.AXTcode),2,1))
ELSE UPPER(AXT.AXTcode) END AS AXT_id,
CONCAT('0x', SUBSTR(UPPER(AXT.AXTfailcode),-2,2)) as AXT_info_byte,
CONCAT('0x', SUBSTR(UPPER(AXT.AXTstatus),-2,2)) AS AXT_status_byte,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-1,1)) IN ('1','3','5','7','9','B','D','F') THEN 1
   ELSE 0
END AS test_failed,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-1,1)) IN ('2','3','6','7','A','B','E','F') THEN 1
   ELSE 0
END AS test_failed_this_operation_cycle,
CASE
 WHEN UPPER(SUBSTRING(AXT.AXTstatus,-1,1)) IN ('4','5','6','7','C','D','E','F') THEN 1
   ELSE 0
END AS pending_AXT,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-1,1)) IN ('8','9','A','B','C','D','E','F') THEN 1
   ELSE 0
END AS confirmed_AXT,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-2,1)) IN ('1','3','5','7','9','B','D','F') THEN 1
   ELSE 0
END AS test_not_completed_since_last_clear,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-2,1)) IN ('2','3','6','7','A','B','E','F') THEN 1
   ELSE 0
END AS test_failed_since_last_clear,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-2,1)) IN ('4','5','6','7','C','D','E','F') THEN 1
   ELSE 0
 END AS test_not_completed_this_operation_cycle,
CASE
   WHEN UPPER(SUBSTRING(AXT.AXTstatus,-2,1)) IN ('8','9','A','B','C','D','E','F') THEN 1
   ELSE 0
END AS warning_indicator_requested,
'VHA' as source,
cvdc60_partition_region_x as partition_region,
cvdc60_partition_country_x as partition_country,
DATE_ADD(to_date(cvdc60_mbld_data_gathered_time_x_2), CAST((-1*(date_format(to_date(cvdc60_mbld_data_gathered_time_x_2) ,'u'))+1) AS INT)) as partition_date,
cvdc60_vd_gps_long_degrees_r_3 as longitude,
cvdc60_vd_gps_lat_degrees_r_3 as latitude,
'' as auth_stat,
'' as raw_payload_metadata_cvrt_ver
from NCVDC60_VHA_VEH_SEC_HTE 
LATERAL VIEW explode(cvdc60_vd_eculist_AXTlist_x_2) explodeVal AS key
LATERAL VIEW explode(key.AXTList) explodeVal AS AXT
where AXT.AXTcode is not null and AXT.AXTstatus IS NOT NULL
and cvdc60_partition_date_x >= substr(date_sub(current_date, ${hivevar:lkb_days}), 1,7) 
and to_date(cvdc60_mbld_data_gathered_time_x_2) >= date_sub(current_date, ${hivevar:lkb_days})) AXTlist
where AXTlist.partition_date >= date_sub(current_date, ${hivevar:lkb_days})
