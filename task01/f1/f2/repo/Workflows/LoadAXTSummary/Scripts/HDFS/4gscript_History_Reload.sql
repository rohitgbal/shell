select
sha_key as cvdsvt_sha_k ,
sha2(CONCAT(COALESCE(vin,""),COALESCE(AXT_id,""),COALESCE(ecu_id,""),COALESCE(cast(event_time as string),"")),256) as cvdsvt_sha_k_AXT,
vin as cvdsvt_vin_d_3,
event_time as cvdsvt_event_s_3,
number as cvdsvt_nump_r_2,
longitude as cvdsvt_lon_r_3,
latitude cvdsvt_lat_r_3,
ecu_id as cvdsvt_ecu_d_2,
AXT_id as cvdsvt_AXT_d_2,
AXT_info_byte as cvdsvt_AXT_info_byte_x_2 ,
AXT_status_byte as cvdsvt_AXT_status_byte_x_2,
test_failed as cvdsvt_test_fald_r_2,
test_failed_this_operation_cycle as cvdsvt_test_fald_this_oper_cyc_r_2,
pending_AXT as cvdsvt_pend_AXT_r_2,
confirmed_AXT as cvdsvt_confmd_AXT_r_2,
test_not_completed_since_last_clear as cvdsvt_test_not_cmpltd_since_lst_clr_r_2,
test_failed_since_last_clear as cvdsvt_test_fald_since_lst_clr_r_2,
test_not_completed_this_operation_cycle as cvdsvt_test_not_cmpltd_this_oper_cyc_r_2,
warning_indicator_requested as cvdsvt_wrng_ind_reqstd_r_2,
source as cvdsvt_src_c,
partition_region as cvdsvt_region_x,
'change_user' as cvdsvt_created_by_c,
from_unixtime(unix_timestamp()) as cvdsvt_created_on_s,
raw_payload_metadata_cvrt_ver as cvdsvt_raw_payload_metadata_cvrt_ver_x,
auth_stat as cvdsvt_auth_stat_c,
tblx01_lifecycmde_d_actl_r as cvdsvt_lifecycmde_d_actl_r,
partition_country as cvdsvt_partition_cntry_x,
partition_date as cvdsvt_partition_date_x
from
(select
tblx01_sha_k AS sha_key,
coalesce(tblx01_vin_d_3,tblx01_calcd_vin_1st_11_r) AS vin,
IF(tblx01_der_event_s_3 > tblx01_msgmetadata_arrival_s, tblx01_msgmetadata_arrival_s, tblx01_der_event_s_3) AS event_time,
tblx01_nump_mstr_val_r AS number,
tblx01_gps_longitude_decimaldegrees_r_3 as longitude,
tblx01_gps_latitude_decimaldegrees_r_3 as latitude,
CONCAT('0x', SUBSTR(UPPER(key.ecuid),-3,3)) AS ecu_id,
CASE WHEN LENGTH(UPPER(AXT.AXTid)) = 4 THEN CONCAT(SUBSTR(UPPER(AXT.AXTid),1,1),'0',SUBSTR(UPPER(AXT.AXTid),2,3))
WHEN LENGTH(UPPER(AXT.AXTid)) = 3 THEN CONCAT(SUBSTR(UPPER(AXT.AXTid),1,1),'00',SUBSTR(UPPER(AXT.AXTid),2,2))
WHEN LENGTH(UPPER(AXT.AXTid)) = 2 THEN CONCAT(SUBSTR(UPPER(AXT.AXTid),1,1),'000',SUBSTR(UPPER(AXT.AXTid),2,1))
ELSE UPPER(AXT.AXTid) END AS AXT_id,
CONCAT('0x', SUBSTR(UPPER(AXT.AXTadditionalInfo),-2,2)) as AXT_info_byte,
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
'CRC4T' as source,
tblx01_partition_region_x as partition_region,
tblx01_partition_country_x as partition_country,
DATE_ADD(tblx01_partition_date_x, CAST((-1*(date_format(tblx01_partition_date_x ,'u'))+1) AS INT)) as partition_date,
tblx01_auth_stat_c as auth_stat,
COALESCE(tblx01_lifecycmde_d_actl_r, LEAD(tblx01_lifecycmde_d_actl_r, 1) OVER (partition by tblx01_vin_d_3 order by tblx01_der_event_s_3, tblx01_nump_mstr_val_r, tblx01_tcu_msg_d)) as tblx01_lifecycmde_d_actl_r ,
tblx01_raw_payload_metadata_cvrt_ver_r as raw_payload_metadata_cvrt_ver
FROM change_database.change_table
LATERAL VIEW explode(tblx01_ecus_x) ecu_array as Key
LATERAL VIEW explode(key.AXTs) AXT_array as AXT
WHERE AXT.AXTid IS NOT NULL and AXT.AXTstatus IS NOT NULL
and tblx01_partition_date_x >= date_sub('current_date', change_lookback)
and not (tblx01_created_on_s >= '2020-02-09 00:00:00.0' and tblx01_created_on_s < '2020-02-13 00:00:00.0' and upper(tblx01_process_stat_dtl_x) like '%AXTID%')
)QRY Where QRY.partition_date >= date_sub('current_date', change_lookback) 
union
select
sha_key as cvdsvt_sha_k ,
reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex', CONCAT(COALESCE(vin,""),COALESCE(AXT_id,""),COALESCE(ecu_id,""),COALESCE(cast(event_time as string),""))) as cvdsvt_sha_k_AXT,
vin as cvdsvt_vin_d_3,
event_time as cvdsvt_event_s_3,
number as cvdsvt_nump_r_2,
longitude as cvdsvt_lon_r_3,
latitude cvdsvt_lat_r_3,
ecu_id as cvdsvt_ecu_d_2,
AXT_id as cvdsvt_AXT_d_2,
AXT_info_byte as cvdsvt_AXT_info_byte_x_2 ,
AXT_status_byte as cvdsvt_AXT_status_byte_x_2,
test_failed as cvdsvt_test_fald_r_2,
test_failed_this_operation_cycle as cvdsvt_test_fald_this_oper_cyc_r_2,
pending_AXT as cvdsvt_pend_AXT_r_2,
confirmed_AXT as cvdsvt_confmd_AXT_r_2,
test_not_completed_since_last_clear as cvdsvt_test_not_cmpltd_since_lst_clr_r_2,
test_failed_since_last_clear as cvdsvt_test_fald_since_lst_clr_r_2,
test_not_completed_this_operation_cycle as cvdsvt_test_not_cmpltd_this_oper_cyc_r_2,
warning_indicator_requested as cvdsvt_wrng_ind_reqstd_r_2,
source as cvdsvt_src_c,
partition_region as cvdsvt_region_x,
'change_user' as cvdsvt_created_by_c,
from_unixtime(unix_timestamp()) as cvdsvt_created_on_s,
raw_payload_metadata_cvrt_ver as cvdsvt_raw_payload_metadata_cvrt_ver_x,
auth_stat as cvdsvt_auth_stat_c,
tblx01_lifecycmde_d_actl_r as cvdsvt_lifecycmde_d_actl_r,
partition_country as cvdsvt_partition_cntry_x,
partition_date as cvdsvt_partition_date_x
from
(select
tblx01_sha_k AS sha_key,
coalesce(tblx01_vin_d_3,tblx01_calcd_vin_1st_11_r) AS vin,
IF(tblx01_der_event_s_3 > tblx01_msgmetadata_arrival_s, tblx01_msgmetadata_arrival_s, tblx01_der_event_s_3) AS event_time,
tblx01_nump_mstr_val_r AS number,
tblx01_gps_longitude_decimaldegrees_r_3 as longitude,
tblx01_gps_latitude_decimaldegrees_r_3 as latitude,
CONCAT('0x', SUBSTR(UPPER(key.ecuid),-3,3)) AS ecu_id,
CASE WHEN LENGTH(UPPER(AXT.AXTid)) = 4 THEN CONCAT(SUBSTR(UPPER(AXT.AXTid),1,1),'0',SUBSTR(UPPER(AXT.AXTid),2,3))
WHEN LENGTH(UPPER(AXT.AXTid)) = 3 THEN CONCAT(SUBSTR(UPPER(AXT.AXTid),1,1),'00',SUBSTR(UPPER(AXT.AXTid),2,2))
WHEN LENGTH(UPPER(AXT.AXTid)) = 2 THEN CONCAT(SUBSTR(UPPER(AXT.AXTid),1,1),'000',SUBSTR(UPPER(AXT.AXTid),2,1))
ELSE UPPER(AXT.AXTid) END AS AXT_id,
CONCAT('0x', SUBSTR(UPPER(AXT.AXTadditionalInfo),-2,2)) as AXT_info_byte,
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
'CRC4T' as source,
tblx01_partition_region_x as partition_region,
tblx01_partition_country_x as partition_country,
DATE_ADD(tblx01_partition_date_x, CAST((-1*(date_format(tblx01_partition_date_x ,'u'))+1) AS INT)) as partition_date,
tblx01_auth_stat_c as auth_stat,
COALESCE(tblx01_lifecycmde_d_actl_r, LEAD(tblx01_lifecycmde_d_actl_r, 1) OVER (partition by tblx01_vin_d_3 order by tblx01_der_event_s_3, tblx01_nump_mstr_val_r, tblx01_tcu_msg_d)) as tblx01_lifecycmde_d_actl_r ,
tblx01_raw_payload_metadata_cvrt_ver_r as raw_payload_metadata_cvrt_ver
FROM change_database.Ntblx01_fcproc_MSG_SEC_HTE_AXT_replay_02_2020
LATERAL VIEW explode(tblx01_ecus_x) ecu_array as Key
LATERAL VIEW explode(key.AXTs) AXT_array as AXT
WHERE AXT.AXTid IS NOT NULL and AXT.AXTstatus IS NOT NULL
and tblx01_partition_date_x >= date_sub('current_date', change_lookback)
)QRY Where QRY.partition_date >= date_sub('current_date', change_lookback) 
