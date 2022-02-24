use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=BI;
set hive.orc.cache.use.soft.references=true;
set hive.tez.java.opts=-Xmx24576m;
set hive.tez.container.size=32768;
set tez.am.resource.memory.mb=16384;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;


insert into NCVDCX8_WIL_SUMM_ACID_SEC_HTE
partition(cvdcx8_partition_cntry_x, cvdcx8_partition_date_x)
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
   auth_stat_c as cvdcx8_auth_stat_c,
   raw_payload_metadata_cvrt_ver_r  as cvdcx8_raw_payload_metadata_cvrt_ver_r,
   wil as cvdcx8_wil_c ,
   wil_description as cvdcx8_wil_desc_x,
   transform_status as cvdcx8_tfm_stat_x,
   'Normal' as cvdcx8_lifecycmde_d_actl_r,
   process_status as cvdcx8_process_stat_c,
   process_status_date_time_utc as cvdcx8_process_stat_utc_s,
   from_unixtime(unix_timestamp()) as cvdcx8_created_on_s,
   "${current_user}"   as cvdcx8_created_by_c,
    partition_country as cvdcx8_partition_cntry_x,
    partition_date as cvdcx8_partition_date_x

   FROM
 (select
  cvdc60_sha_k AS sha_key,
  cvdc60_vd_vin_r_2 as vin ,
  IF(cvdc60_calcd_data_gathered_s_2 > cvdc60_ap_calcd_message_datetime_s_2,cvdc60_ap_calcd_message_datetime_s_2,cvdc60_calcd_data_gathered_s_2) as event_time,
  cvdc60_vd_number_uds_metric_r_2 as number,
  cvdc60_vd_gps_long_degrees_r_3 as longitude,
  cvdc60_vd_gps_lat_degrees_r_3 as latitude,
  'VHA' as source,
  cvdc60_partition_region_x as partition_region,
   null as auth_stat_c,
   null  as raw_payload_metadata_cvrt_ver_r,
  wil.code  as wil,
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
  'SUCCESS' as transform_status,
  cvdc60_process_stat_c as process_status,
  cvdc60_process_stat_utc_s as process_status_date_time_utc,
  from_unixtime(unix_timestamp()) as cvdcx8_created_on_s,
  "${current_user}"   as cvdcx8_created_by_c,
  cvdc60_partition_country_x as partition_country,
  DATE_ADD(to_date(cvdc60_mbld_data_gathered_time_x_2), CAST((-1*(date_format(to_date(cvdc60_mbld_data_gathered_time_x_2) ,'u'))+1) AS INT)) as partition_date
  FROM NCVDC60_VHA_VEH_SEC_HTE
  LATERAL VIEW explode(cvdc60_calculated_warning_codes_x_2) wilarray as wil where ((wil.code is not null and wil.code !='' and (SUBSTRING(wil.code,LENGTH(wil.code)-1,2)  <= 31 )
  and cvdc60_partition_date_x >= substr(date_sub('${hivevar:current_date}', ${hivevar:lkb_days}), 1,7) 
  and to_date(cvdc60_mbld_data_gathered_time_x_2)  >= date_sub('${hivevar:current_date}', ${hivevar:lkb_days})))) q1
 where q1.partition_date>=date_sub('${hivevar:current_date}', ${hivevar:lkb_days});

