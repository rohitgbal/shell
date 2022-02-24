from __future__ import print_function
from pyspark.sql import SparkSession
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.functions as func
import pyspark.sql.functions as sf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col,when
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from datetime import datetime
from pyspark.sql.functions import col,row_number
from dateutil.relativedelta import *
from pyspark.sql.window import WindowSpec
from pyspark.sql.types import *
from pyspark import since
from pyspark.sql.functions import regexp_replace,col
from pyspark.sql.window import Window
import argparse
from pyspark_llap.sql.session import HiveWarehouseSession

#variables

hive_database_name = sys.argv[1]
MKD_settings_query_days = sys.argv[2]
currentDate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
output = sys.argv[4]
MKD_settings_query_day = int(MKD_settings_query_days)
startDateTime = (currentDate+relativedelta(days=-MKD_settings_query_day)).strftime('%Y-%m-%d')

print ('Output********', output)
print ('database:', hive_database_name)
print ('MKD_settings_query_day:', MKD_settings_query_day)
print ('currentDate:', currentDate)
print ('startDateTime:', startDateTime)


if __name__ == "__main__":

     spark = SparkSession.builder.appName("MKD Settings Summary PJ1").enableHiveSupport().getOrCreate()

     hive = HiveWarehouseSession.session(spark).build()

     MKDsettings_sql=hive.executeQuery(" SELECT tblx01_vin_d_3 as vin  \
     ,tblx01_MKDstate_x_3 as MKD_state   \
     ,first_timestamp as start_timestamp    \
     ,COALESCE(Lead(first_timestamp,1) OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp),last_timestamp,first_timestamp) AS end_timestamp   \
     ,CASE  \
     WHEN row_number() OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp DESC) = 1 THEN 'NEW' \
     ELSE 'OLD'  \
     END AS record_status   \
     ,partition_region   \
     ,partition_country    \
     ,(SUBSTR(first_timestamp,1,7)) as partition_year_month     \
     ,IF((COALESCE(Lead(first_timestamp,1) OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp),last_timestamp,first_timestamp) >= Date_sub(CURRENT_DATE,30) OR Row_number() OVER(partition BY tblx01_vin_d_3 ORDER BY first_timestamp DESC) = 1),'NEW','OLD') as partition_status   \
     FROM ( \
     SELECT   tblx01_vin_d_3   \
     ,tblx01_MKDstate_x_3   \
     ,min(tblx01_der_event_s_3) AS first_timestamp   \
     ,max(tblx01_der_event_s_3) AS last_timestamp   \
     ,abs_diff   \
     ,partition_country   \
     ,partition_region   \
     FROM  (  \
     SELECT    \
     tblx01_vin_d_3    \
     ,tblx01_MKDstate_x_3    \
     ,tblx01_der_event_s_3   \
     ,abs(record_id - record_id_by_state) AS abs_diff   \
     ,last_value(tblx01_partition_country_x, true) OVER(partition BY tblx01_vin_d_3,tblx01_MKDstate_x_3, abs(record_id - record_id_by_state) ORDER BY tblx01_der_event_s_3 rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS partition_country   \
     ,last_value(tblx01_partition_region_x, true) OVER(partition BY tblx01_vin_d_3,tblx01_MKDstate_x_3, abs(record_id  - record_id_by_state) ORDER BY tblx01_der_event_s_3 rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS partition_region   \
     FROM( \
     select  tblx01_vin_d_3 \
     ,tblx01_MKDstate_x_3 \
     ,tblx01_der_event_s_3 \
     ,row_number() OVER(partition BY tblx01_vin_d_3 ORDER BY tblx01_der_event_s_3,row_id) AS record_id \
     ,row_number() OVER(partition BY tblx01_vin_d_3, tblx01_MKDstate_x_3 ORDER BY tblx01_der_event_s_3,row_id) AS record_id_by_state \
     ,row_id \
     ,tblx01_partition_country_x \
     ,tblx01_partition_region_x \
     FROM (SELECT TMP1.*, ROW_NUMBER() OVER (PARTITION BY tblx01_vin_d_3 ORDER BY tblx01_der_event_s_3,tblx01_MKDstate_x_3 ) AS row_id \
     FROM( \
     SELECT   tblx01_vin_d_3    \
     ,tblx01_MKDstate_x_3   \
     ,tblx01_der_event_s_3   \
     ,tblx01_partition_country_x   \
     ,tblx01_region_x as tblx01_partition_region_x  \
     FROM {0}.Ntblx01_CRC4T_MSG_PJ1_SEC_HTE   \
     WHERE  LENGTH(tblx01_vin_d_3)==17  \
     AND tblx01_MKDstate_x_3 IS NOT NULL   \
     AND (lower(tblx01_msg_metadata_msg_n)=lower('MKDSettingsStatusAlert') or lower(tblx01_functionmsgname_x_3)= lower('MKDSettingsStatusAlert')) AND tblx01_partition_date_x  >= '{2}'  \
     UNION   \
     SELECT  \
     scvp87_vin_d_2 as tblx01_vin_d_3   \
     ,scvp87_tcx_st_x_2 as tblx01_MKDstate_x_3   \
     ,scvp87_strt_s_2   as tblx01_der_event_s_3   \
     ,scvp87_partition_country_x as tblx01_partition_country_x   \
     ,scvp87_partition_region_x as tblx01_partition_region_x   \
     FROM {0}.NSCVP87_tcx_SETG_PJ1_PII_HTE  where scvp87_partition_status_x='NEW' \
     UNION  \
     SELECT scvp87_vin_d_2      AS tblx01_vin_d_3  \
     ,scvp87_tcx_st_x_2          AS tblx01_MKDstate_x_3  \
     ,scvp87_end_s_2             AS tblx01_der_event_s_3  \
     ,scvp87_partition_country_x AS tblx01_partition_country_x  \
     ,scvp87_partition_region_x  AS tblx01_partition_region_x  \
     FROM  {0}.NSCVP87_tcx_SETG_PJ1_PII_HTE \
     WHERE  scvp87_partition_status_x = 'NEW' and scvp87_rec_stat_x_2 = 'NEW')TMP1) TMP2 ) TMP3) TMP4   \
     GROUP BY tblx01_vin_d_3, tblx01_MKDstate_x_3, abs_diff, partition_region, partition_country) Final".format(hive_database_name,MKD_settings_query_day,startDateTime))

     MKDsettings_sql.write.format("orc").mode("overwrite").save(output)