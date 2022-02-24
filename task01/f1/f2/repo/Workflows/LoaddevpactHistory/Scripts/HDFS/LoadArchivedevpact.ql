use ${hivevar:db_name};

set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.tez.container.size=10240;
set hive.tez.java.opts=-Xmx8192m;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;




INSERT INTO TABLE NSCVSFY_tcx_HIST_ARCHV_ST_HTE  PARTITION(created_date)
Select  sha_key,
		vin,
   MKDactivescheduletimestamp,
   derived_event_timestamp,
   msgmetadata_messagename,
   functionmsgname,
   partition_region_x,
   partition_country_x,
   partition_date_x,
   to_date(from_unixtime(unix_timestamp())) as created_date
From NSCVSFY_TEMP_tcx_HIST_ST_HTE;

