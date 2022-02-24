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
set tez.am.resource.memory.mb=32768;
set hive.tez.container.size=32768;
set hive.exec.orc.split.strategy=BI;


INSERT INTO TABLE ${staging_table}
SELECT tblx01_vin_d_3 as vin   
,tblx01_MKDstate_x_3 as MKD_state    
,first_timestamp as start_timestamp     
,COALESCE(Lead(first_timestamp,1) OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp),last_timestamp,first_timestamp) AS end_timestamp    
,CASE   
WHEN row_number() OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp DESC) = 1 THEN 'NEW'  
ELSE 'OLD'   
END AS record_status    
,partition_region    
,partition_country     
,(SUBSTR(first_timestamp,1,7)) as partition_year_month      
,IF((COALESCE(Lead(first_timestamp,1) OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp),last_timestamp,first_timestamp) >= Date_sub(CURRENT_DATE,30) OR Row_number() OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp DESC) = 1),'NEW','OLD') as partition_status    
FROM (  
SELECT   tblx01_vin_d_3    
,tblx01_MKDstate_x_3    
,min(tblx01_der_event_s_3) AS first_timestamp    
,max(tblx01_der_event_s_3) AS last_timestamp    
,abs_diff    
,partition_country    
,partition_region    
FROM  (   
SELECT     
tblx01_vin_d_3     
,tblx01_MKDstate_x_3     
,tblx01_der_event_s_3    
,abs(record_id - record_id_by_state) AS abs_diff    
,last_value(tblx01_partition_country_x, true) OVER(partition BY tblx01_vin_d_3,tblx01_MKDstate_x_3, abs(record_id - record_id_by_state) ORDER BY tblx01_der_event_s_3 rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS partition_country    
,last_value(tblx01_partition_region_x, true) OVER(partition BY tblx01_vin_d_3,tblx01_MKDstate_x_3, abs(record_id  - record_id_by_state) ORDER BY tblx01_der_event_s_3 rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS partition_region    
FROM(  
select  tblx01_vin_d_3  
,tblx01_MKDstate_x_3  
,tblx01_der_event_s_3  
,row_number() OVER(partition BY tblx01_vin_d_3 ORDER BY tblx01_der_event_s_3,row_id) AS record_id  
,row_number() OVER(partition BY tblx01_vin_d_3, tblx01_MKDstate_x_3 ORDER BY tblx01_der_event_s_3,row_id) AS record_id_by_state  
,row_id  
,tblx01_partition_country_x  
,tblx01_partition_region_x  
FROM (SELECT TMP1.*, ROW_NUMBER() OVER (PARTITION BY tblx01_vin_d_3 ORDER BY tblx01_der_event_s_3,tblx01_MKDstate_x_3 ) AS row_id  
FROM(  
SELECT   tblx01_vin_d_3     
,tblx01_MKDstate_x_3    
,tblx01_der_event_s_3    
,tblx01_partition_country_x    
,tblx01_region_x as tblx01_partition_region_x   
FROM ${source_table}    
WHERE  LENGTH(tblx01_vin_d_3)==17   
AND tblx01_MKDstate_x_3 IS NOT NULL    
AND (lower(tblx01_msg_metadata_msg_n)=lower('MKDSettingsStatusAlert') or lower(tblx01_functionmsgname_x_3)= lower('MKDSettingsStatusAlert')) AND tblx01_partition_date_x  >=   date_sub('${hivevar:current_date}', ${hivevar:MKD_settings_query_days})  
UNION    
SELECT   
scvpep_vin_d_2              AS tblx01_vin_d_3    
,scvpep_tcx_st_x_2          AS tblx01_MKDstate_x_3    
,scvpep_strt_s_2            AS tblx01_der_event_s_3    
,scvpep_partition_country_x AS tblx01_partition_country_x    
,scvpep_partition_region_x  AS tblx01_partition_region_x    
FROM ${master_table}  where scvpep_partition_status_x='NEW'  
UNION   
SELECT scvpep_vin_d_2       AS tblx01_vin_d_3   
,scvpep_tcx_st_x_2          AS tblx01_MKDstate_x_3   
,scvpep_end_s_2             AS tblx01_der_event_s_3   
,scvpep_partition_country_x AS tblx01_partition_country_x   
,scvpep_partition_region_x  AS tblx01_partition_region_x   
FROM  ${master_table} where  scvpep_partition_status_x = 'NEW' and scvpep_rec_stat_x_2 = 'NEW')TMP1) TMP2 ) TMP3) TMP4    
GROUP BY tblx01_vin_d_3, tblx01_MKDstate_x_3, abs_diff, partition_region, partition_country) Final;