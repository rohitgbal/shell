from __future__ import print_function
from pyspark.sql import SparkSession
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.functions as func
import pyspark.sql.functions as sf
from pyspark.sql.functions import lit
from pyspark.sql.functions import col,when
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import time
import datetime
from datetime import datetime
from pyspark.sql.functions import col,row_number
from dateutil.relativedelta import *

database = sys.argv[1]
user = sys.argv[2]
CRC4T_strgypartno_query_days = sys.argv[3]
currentDate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
CRC4T_strgypartno_query_day = int(CRC4T_strgypartno_query_days)
startDateTime = (currentDate+relativedelta(days=-CRC4T_strgypartno_query_day)).strftime('%Y-%m-%d')

print ('database:', database)
print ('user:', user)
print ('CRC4T_strgypartno_query_day:', CRC4T_strgypartno_query_day)
print ('currentDate:', currentDate)
print ('startDateTime:', startDateTime)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PACETransFormation_TCUStratPartNo_STG").enableHiveSupport().getOrCreate()


    interimsql=spark.sql("""INSERT INTO TABLE {0}.NCVDSUL_PACE_TCU_STRAT_PART_NO_ST_HTE \
                                SELECT vin as cvdcul_vin_d_3 \
                                ,tcu_strategypartnumber as cvdcul_tcu_strat_part_no_c_3 \
                                ,cvrt_version as cvdcul_cvrt_ver_c_3 \
                                ,IF(Lag(event_ts, 1) OVER (partition BY vin ORDER BY event_ts ASC) is null, cast('1990-01-01 00:00:00' as timestamp), event_ts) AS cvdcul_first_assoc_s_3 \
                                ,Lead(event_ts, 1, '2999-12-31 23:59:59') OVER (partition BY vin ORDER BY event_ts ASC) AS cvdcul_last_assoc_s_3 \
                                ,event_ts as cvdcul_event_s_3 \
                                ,latest_message_name as cvdcul_latest_msg_n_3 \
                                ,partition_country as cvdcul_partition_country_x \
                                ,FROM_UNIXTIME(UNIX_TIMESTAMP()) as cvdcul_created_on_s \
                                ,'{1}' AS cvdcul_created_by_c \
                                FROM \
                                (SELECT vin,tcu_strategypartnumber,latest_message_name,Min(event_ts) AS event_ts,cvrt_version, partition_country  \
                                FROM \
                                (SELECT vin,tcu_strategypartnumber,event_ts,last_value(message_name) OVER(partition BY vin, tcu_strategypartnumber ORDER BY event_ts ASC rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS latest_message_name,last_value(cvrt_version) OVER(partition BY vin, tcu_strategypartnumber ORDER BY event_ts ASC rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS cvrt_version,Row_Number() over(partition by vin, tcu_strategypartnumber order by event_ts ASC) as Strat_row_no,Row_Number() over(partition by vin order by event_ts ASC) as vin_row_no, partition_country  \
                                FROM \
                                (SELECT COALESCE(tmsg.vin,cmsg.vin) AS vin,tmsg.strat_no AS tcu_strategypartnumber,IF(tmsg.event_ts > tmsg.arr_ts, tmsg.arr_ts, tmsg.event_ts) AS event_ts,tmsg.message_name,COALESCE(tmsg.payload_cvrt_version,tmsg.cvrt_version) AS cvrt_version, tmsg.partition_country \
                                FROM ( \
                                SELECT DISTINCT \
                                tblx01_vin_d_3                         AS vin, \
                                tblx01_msg_metadata_msg_n              AS message_name, \
                                tblx01_app_crltn_d_3                   AS appcorrelationid, \
                                tblx01_strat_part_num_x                AS strat_no, \
                                tblx01_der_event_s_3                   AS event_ts, \
                                tblx01_msgmetadata_arrival_s           AS arr_ts, \
                                tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version, \
                                tblx01_pro_to_file_ver_x               AS payload_cvrt_version, \
                                tblx01_anonymization_status_c_3        AS anon_flag, \
                                tblx01_partition_region_x              AS partition_region, \
                                tblx01_partition_country_x             AS partition_country \
                                FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_SEC_HTE \
                                WHERE tblx01_strat_part_num_x IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Alert','CommandResponse','Query') AND tblx01_partition_date_x  >= '{2}' \
                                UNION \
                                DISTINCT \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3                         AS vin, \
                                tblx01_msg_metadata_msg_n              AS message_name, \
                                tblx01_app_crltn_d_3                   AS appcorrelationid, \
                                tblx01_strat_part_num_x                AS strat_no, \
                                tblx01_der_event_s_3                   AS event_ts, \
                                tblx01_msgmetadata_arrival_s           AS arr_ts, \
                                tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version, \
                                tblx01_pro_to_file_ver_x               AS payload_cvrt_version, \
                                tblx01_anonymization_status_c_3        AS anon_flag, \
                                tblx01_partition_region_x              AS partition_region, \
                                tblx01_partition_country_x             AS partition_country \
                                FROM {0}.Ntblx01_fcproc_MSG_PRS_RAWDATA_k2_SEC_HTE \
                                WHERE tblx01_strat_part_num_x IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Alert','CommandResponse','Query') AND tblx01_partition_date_x  >= '{2}' \
                                UNION \
                                DISTINCT \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3                         AS vin, \
                                tblx01_msg_metadata_msg_n              AS message_name, \
                                tblx01_app_crltn_d_3                   AS appcorrelationid, \
                                tblx01_strat_part_num_x                AS strat_no, \
                                tblx01_der_event_s_3                   AS event_ts, \
                                tblx01_msgmetadata_arrival_s           AS arr_ts, \
                                tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version, \
                                tblx01_pro_to_file_ver_x               AS payload_cvrt_version, \
                                tblx01_anonymization_status_c_3        AS anon_flag, \
                                tblx01_partition_region_x              AS partition_region, \
                                tblx01_partition_country_x             AS partition_country \
                                FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_PSA_SEC_HTE \
                                WHERE tblx01_strat_part_num_x IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Alert','CommandResponse','Query') AND tblx01_partition_date_x  >= '{2}' \
                                UNION \
                                DISTINCT \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3                         AS vin, \
                                tblx01_msg_metadata_msg_n              AS message_name, \
                                tblx01_app_crltn_d_3                   AS appcorrelationid, \
                                tblx01_strat_part_num_x                AS strat_no, \
                                tblx01_der_event_s_3                   AS event_ts, \
                                tblx01_msgmetadata_arrival_s           AS arr_ts, \
                                tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version, \
                                tblx01_pro_to_file_ver_x               AS payload_cvrt_version, \
                                tblx01_anonymization_status_c_3        AS anon_flag, \
                                tblx01_partition_region_x              AS partition_region, \
                                tblx01_partition_country_x             AS partition_country \
                                FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_PSU_SEC_HTE \
                                WHERE tblx01_strat_part_num_x IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Alert','CommandResponse','Query') AND tblx01_partition_date_x  >= '{2}' \
                                ) tmsg  \
                                LEFT JOIN \
                                ( \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3            AS vin, \
                                tblx01_app_crltn_d_3      AS appcorrelationid, \
                                tblx01_msg_metadata_msg_n AS message_name \
                                FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_SEC_HTE \
                                WHERE tblx01_vin_d_3 IS NOT NULL AND tblx01_app_crltn_d_3 IS NOT NULL AND tblx01_msg_metadata_msg_n IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Command','QueryResponse') AND tblx01_partition_date_x  >= '{2}' \
                                UNION \
                                DISTINCT \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3            AS vin, \
                                tblx01_app_crltn_d_3      AS appcorrelationid, \
                                tblx01_msg_metadata_msg_n AS message_name \
                                FROM {0}.Ntblx01_fcproc_MSG_PRS_RAWDATA_k2_SEC_HTE \
                                WHERE tblx01_vin_d_3 IS NOT NULL AND tblx01_app_crltn_d_3 IS NOT NULL AND tblx01_msg_metadata_msg_n IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Command','QueryResponse') AND tblx01_partition_date_x  >= '{2}' \
                                UNION \
                                DISTINCT \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3            AS vin, \
                                tblx01_app_crltn_d_3      AS appcorrelationid, \
                                tblx01_msg_metadata_msg_n AS message_name \
                                FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_PSA_SEC_HTE \
                                WHERE tblx01_vin_d_3 IS NOT NULL AND tblx01_app_crltn_d_3 IS NOT NULL AND tblx01_msg_metadata_msg_n IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Command','QueryResponse') AND tblx01_partition_date_x  >= '{2}' \
                                UNION \
                                DISTINCT \
                                SELECT DISTINCT  \
                                tblx01_vin_d_3            AS vin, \
                                tblx01_app_crltn_d_3      AS appcorrelationid, \
                                tblx01_msg_metadata_msg_n AS message_name \
                                FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_PSU_SEC_HTE \
                                WHERE tblx01_vin_d_3 IS NOT NULL AND tblx01_app_crltn_d_3 IS NOT NULL AND tblx01_msg_metadata_msg_n IS NOT NULL AND tblx01_msg_metadata_msg_typ_x IN ('Command','QueryResponse') AND tblx01_partition_date_x  >= '{2}' \
                                ) cmsg \
                                ON tmsg.appcorrelationid = cmsg.appcorrelationid AND tmsg.message_name = cmsg.message_name \
                                UNION \
                                SELECT cvdcul_vin_d_3 as vin,cvdcul_tcu_strat_part_no_c_3 as tcu_strategypartnumber,cvdcul_event_s_3 as event_ts,cvdcul_latest_msg_n_3 AS message_name,cvdcul_cvrt_ver_c_3 as cvrt_version, cvdcul_partition_country_x as partition_country FROM {0}.NCVDCUL_PACE_TCU_STRAT_PART_NO_SEC_HTE hload) dltload \
                                WHERE    vin IS NOT NULL \
                                ) bmsgs \
                                GROUP BY vin,tcu_strategypartnumber,latest_message_name,cvrt_version,partition_country,ABS(vin_row_no-Strat_row_no)) nbmsgs""".format(database,user,startDateTime))
