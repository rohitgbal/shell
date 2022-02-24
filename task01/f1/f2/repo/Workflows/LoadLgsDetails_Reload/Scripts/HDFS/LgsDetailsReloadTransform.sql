select
sha_key as cvdcx8_src_sha_k ,
sha2(CONCAT(COALESCE(vin,""),COALESCE(cast(event_time as string),""),COALESCE(wil,"")),256) as cvdcx8_wil_sha_k,
vin as cvdcx8_vin_d_2,
event_time as cvdcx8_event_s,
number as cvdcx8_nump_r_2,
longitude as cvdcx8_long_r_3,
latitude cvdcx8_lat_r_3,
source as cvdcx8_src_c,
partition_region as cvdcx8_partition_rgn_x,
 auth_stat_c as  cvdcx8_auth_stat_c ,
 raw_payload_metadata_cvrt_ver_r as   cvdcx8_raw_payload_metadata_cvrt_ver_r ,
wil as cvdcx8_wil_c ,
wil_description as cvdcx8_wil_desc_x,
transform_status as cvdcx8_tfm_stat_x,
tblx01_lifecycmde_d_actl_r as lifecycmde_d_actl_r,
process_status as cvdcx8_process_stat_c,
process_status_date_time_utc as cvdcx8_process_stat_utc_s,
from_unixtime(unix_timestamp()) as cvdcx8_created_on_s,
'change_user' as cvdcx8_created_by_c,
partition_country as cvdcx8_partition_cntry_x,
partition_date as cvdcx8_partition_date_x

from
(select
 tblx01_sha_k AS sha_key,
 coalesce(tblx01_vin_d_3,tblx01_calcd_vin_1st_11_r) AS vin,
 IF(tblx01_der_event_s_3 > tblx01_msgmetadata_arrival_s, tblx01_msgmetadata_arrival_s, tblx01_der_event_s_3) AS event_time,
 tblx01_nump_mstr_val_r AS number,
 tblx01_gps_longitude_decimaldegrees_r_3 as longitude,
 tblx01_gps_latitude_decimaldegrees_r_3 as latitude,
 'CRC4T' as source,
 tblx01_region_x as partition_region,
 tblx01_auth_stat_c as auth_stat_c,
 tblx01_raw_payload_metadata_cvrt_ver_r as raw_payload_metadata_cvrt_ver_r,
  wil.code as wil,
 CASE
 WHEN (wil.code = '600E00' ) THEN 'Forward Collision Warning'
 WHEN (wil.code = '600E01' ) THEN 'Park Aid Malfunction Warning'
 WHEN (wil.code = '600E02' ) THEN 'Air Filter Minder Warning'
 WHEN (wil.code = '600E003') THEN 'All Wheel Drive OFF Warning'
 WHEN (wil.code = '600E05' ) THEN 'Diesel Engine Idle Shutdown Warning'
 WHEN (wil.code = '600E06' ) THEN 'Diesel Engine Warming Warning'
 WHEN (wil.code = '600E07' ) THEN 'Diesel Particulate Filter Cleaning'
 WHEN (wil.code = '600E08' ) THEN 'Diesel Exhaust Overtemperature'
 WHEN (wil.code = '600E09' ) THEN 'Diesel Pre-Heat Warning'
 WHEN (wil.code = '600E10' ) THEN 'Diesel Water In Fuel Warning'
 WHEN (wil.code = '600E11' ) THEN 'Fuel Door Open or Ajar Warning'
 WHEN (wil.code = '600E12' ) THEN 'Check Fuel Fill Inlet Warning'
 WHEN (wil.code = '600E13' ) THEN 'Check Fuel Cap Warning'
 WHEN (wil.code = '600E14' ) THEN 'Adaptive Cruise Control Warning'
 WHEN (wil.code = '600E15' ) THEN 'Powertrain Malfunction Warning'
 WHEN (wil.code = '600E16' ) THEN 'Engine Coolant Overtemperature'
 WHEN (wil.code = '600E17' ) THEN 'Low Engine Oil Pressure Warning'
 WHEN (wil.code = '600E18' ) THEN 'Electric Trailer Brake Connection Status'
 WHEN (wil.code = '600E19' ) THEN 'Low Washer Fluid Warning'
 WHEN (wil.code = '600E20' ) THEN 'Low Fuel Warning'
 WHEN (wil.code = '600E21' ) THEN 'Anti Theft Indicator or Warning'
 WHEN (wil.code = '600E22' ) THEN 'Air Suspension or Ride Control Fault'
 WHEN (wil.code = '600E23' ) THEN 'Traction Control (TC) or Interactive Vehicle Dynamics (IVD) or Ride Stability Control (RSC) OFF Indication'
 WHEN (wil.code = '600E24' ) THEN 'Traction Control (TC) or Interactive Vehicle Dynamics (IVD) or Ride Stability Control (RSC) Event or Fault'
 WHEN (wil.code = '600E25' ) THEN 'Charge System Fault'
 WHEN (wil.code = '600E26' ) THEN 'Malfunction Indicator Lamp (MIL)'
 WHEN (wil.code = '600E27' ) THEN 'Tire Pressure Monitor System (TPMS)'
 WHEN (wil.code = '600E28' ) THEN 'Brake Warning'
 WHEN (wil.code = '600E29' ) THEN 'Antilock Brake Fault'
 WHEN (wil.code = '600E30' ) THEN 'Restraints Indicator Lamp (RIL) Warning'
 WHEN (wil.code = '600E31' ) THEN 'Fasten Seatbelt Warning'
 WHEN (wil.code = '600F30' ) THEN 'Blind Spot Detection Warning'
 WHEN (wil.code = '600F29' ) THEN 'Service Steering Warning'
 WHEN (wil.code = '600F28' ) THEN 'Not used.  (formerly HEV Hazard Warning on U377)'
 WHEN (wil.code = '600F27' ) THEN 'Diesel Exhaust Fluid Level Low Warnings'
 WHEN (wil.code = '600F26' ) THEN 'Diesel Exhaust Fluid Contaminated Warnings'
 WHEN (wil.code = '600F25' ) THEN 'Hill Descent Control Fault Warning'
 WHEN (wil.code = '600F24' ) THEN 'Lane Departure Warning'
 WHEN (wil.code = '600F23' ) THEN 'City Safety Warning'
 WHEN (wil.code = '600F22' ) THEN 'Hill Start Assist Warning'
 WHEN (wil.code = '600F21' ) THEN 'Bulb Failure'
 WHEN (wil.code = '600F20' ) THEN 'Start/Stop Engine Warning'
 WHEN (wil.code = '600F19' ) THEN 'FHEV Hazard Warning'
 ElSE null END AS  wil_description ,
 case
 when ( ( (wil.code rlike '600E') OR (wil.code rlike '600F') )   AND  ((tblx01_raw_payload_metadata_cvrt_ver_r rlike '3.0.6') OR
 (tblx01_raw_payload_metadata_cvrt_ver_r rlike'3.0.5') ) )
 then 'TransFail'
 Else 'Success' END AS transform_status,
 COALESCE(tblx01_lifecycmde_d_actl_r, LEAD(tblx01_lifecycmde_d_actl_r, 1) OVER (partition by tblx01_vin_d_3 order by tblx01_der_event_s_3, tblx01_nump_mstr_val_r, tblx01_tcu_msg_d)) as tblx01_lifecycmde_d_actl_r ,
 tblx01_process_stat_c as process_status,
 tblx01_process_stat_utc_s as process_status_date_time_utc,
 from_unixtime(unix_timestamp()) as cvdcx8_created_on_s,
 'change_user' as cvdcx8_created_by_c,
 tblx01_partition_country_x as partition_country,
 DATE_ADD(tblx01_der_event_s_3, CAST((-1*(date_format(tblx01_der_event_s_3 ,'u'))+1) AS INT)) as partition_date
 FROM change_database.change_table
  LATERAL VIEW explode(tblx01_wil_x) wilarray as wil where (tblx01_partition_month_x >= substring('start_date',1,7) and to_date(tblx01_der_event_s_3) >= date_sub('current_date',change_lookback) and (wil.code is not null and wil.code !='' and (SUBSTRING(wil.code,LENGTH(wil.code)-1,2)  <= 31 )))) QRY
   where QRY.partition_date>=date_sub('current_date',change_lookback)