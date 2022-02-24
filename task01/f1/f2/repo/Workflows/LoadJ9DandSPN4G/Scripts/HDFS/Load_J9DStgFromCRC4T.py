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

database = sys.argv[1]
CRC4T_J9D_query_days = sys.argv[2]
output = sys.argv[3]


if __name__ == "__main__":

	spark = SparkSession.builder.appName("PACETransFormation_CRC4T_J9D_STG").enableHiveSupport().getOrCreate()
	spark.conf.set("spark.sql.orc.enabled","true")
	spark.conf.set("spark.sql.hive.convertMetastoreOrc","true")
	spark.conf.set("spark.sql.orc.char.enabled","true")

	cmd_and_response_df = spark.sql("SELECT DISTINCT \
	COALESCE(b.source,a.tblx01_msg_metadata_rgn_n) AS source \
	,a.tblx01_msg_metadata_msg_typ_x as msg_type \
	,a.tblx01_cloud_msg_d \
    ,a.tblx01_app_crltn_d_3 \
	,a.tblx01_wakeupsmsinvoked_r_3 AS wakeupsms_flag \
	,a.tblx01_vin_d_3 AS vin \
    ,a.tblx01_calcd_vin_1st_11_r AS vin_11 \
	,a.tblx01_esn_r AS esn \
	,a.tblx01_msg_metadata_msg_n AS cmdname \
	,a.tblx01_stat_c AS status \
	,a.tblx01_der_event_s_3 AS messagedate \
	,a.tblx01_msgmetadata_arrival_s AS arr_date \
	,a.tblx01_err_c AS cvrterrorcode \
	,a.tblx01_err_x AS errordetail \
	,a.tblx01_correlation_d AS correlationid \
	,a.tblx01_app_crltn_d_3 AS appcorrelationid \
	,b.activityid \
	,a.tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version \
	,a.tblx01_pro_to_file_ver_x AS payload_cvrt_version \
	,a.tblx01_auth_stat_c AS auth_stat \
	,a.tblx01_anonymization_status_c_3 AS anon_flag \
	,a.tblx01_partition_region_x AS partition_region \
	,a.tblx01_partition_country_x AS partition_country \
	FROM {0}.ntblx01_fcproc_msg_rawdata_k2_sec_hte a lateral VIEW json_tuple(a.tblx01_msg_metadata_x_2, 'tag', 'ActivityID') b AS source,activityid \
	WHERE a.tblx01_msg_metadata_msg_typ_x in ('Command','CommandResponse') AND a.tblx01_partition_date_x >= date_sub(current_date, {1}) \
	UNION \
	DISTINCT \
	SELECT DISTINCT \
	COALESCE(b.source,a.tblx01_msg_metadata_rgn_n) AS source \
	,a.tblx01_msg_metadata_msg_typ_x as msg_type \
	,a.tblx01_cloud_msg_d \
    ,a.tblx01_app_crltn_d_3 \
	,a.tblx01_wakeupsmsinvoked_r_3 AS wakeupsms_flag \
	,a.tblx01_vin_d_3 AS vin \
    ,a.tblx01_calcd_vin_1st_11_r AS vin_11 \
	,a.tblx01_esn_r AS esn \
	,a.tblx01_msg_metadata_msg_n AS cmdname \
	,a.tblx01_stat_c AS status \
	,a.tblx01_der_event_s_3 AS messagedate \
	,a.tblx01_msgmetadata_arrival_s AS arr_date \
	,a.tblx01_err_c AS cvrterrorcode \
	,a.tblx01_err_x AS errordetail \
	,a.tblx01_correlation_d AS correlationid \
	,a.tblx01_app_crltn_d_3 AS appcorrelationid \
	,b.activityid \
	,a.tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version \
	,a.tblx01_pro_to_file_ver_x AS payload_cvrt_version \
	,a.tblx01_auth_stat_c AS auth_stat \
	,a.tblx01_anonymization_status_c_3 AS anon_flag \
	,a.tblx01_partition_region_x AS partition_region \
	,a.tblx01_partition_country_x AS partition_country \
	FROM {0}.Ntblx01_fcproc_MSG_PRS_RAWDATA_k2_SEC_HTE a lateral VIEW json_tuple(a.tblx01_msg_metadata_x_2, 'tag', 'ActivityID') b AS source,activityid \
	WHERE a.tblx01_msg_metadata_msg_typ_x in ('Command','CommandResponse') AND a.tblx01_partition_date_x >= date_sub(current_date, {1}) \
	UNION \
	DISTINCT \
	SELECT DISTINCT \
	COALESCE(b.source,a.tblx01_msg_metadata_rgn_n) AS source \
	,a.tblx01_msg_metadata_msg_typ_x as msg_type \
	,a.tblx01_cloud_msg_d \
    ,a.tblx01_app_crltn_d_3 \
	,a.tblx01_wakeupsmsinvoked_r_3 AS wakeupsms_flag \
	,a.tblx01_vin_d_3 AS vin \
    ,a.tblx01_calcd_vin_1st_11_r AS vin_11 \
	,a.tblx01_esn_r AS esn \
	,a.tblx01_msg_metadata_msg_n AS cmdname \
	,a.tblx01_stat_c AS status \
	,a.tblx01_der_event_s_3 AS messagedate \
	,a.tblx01_msgmetadata_arrival_s AS arr_date \
	,a.tblx01_err_c AS cvrterrorcode \
	,a.tblx01_err_x AS errordetail \
	,a.tblx01_correlation_d AS correlationid \
	,a.tblx01_app_crltn_d_3 AS appcorrelationid \
	,b.activityid \
	,a.tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version \
	,a.tblx01_pro_to_file_ver_x AS payload_cvrt_version \
	,a.tblx01_auth_stat_c AS auth_stat \
	,a.tblx01_anonymization_status_c_3 AS anon_flag \
	,a.tblx01_partition_region_x AS partition_region \
	,a.tblx01_partition_country_x AS partition_country \
	FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_PSA_SEC_HTE a lateral VIEW json_tuple(a.tblx01_msg_metadata_x_2, 'tag', 'ActivityID') b AS source,activityid \
	WHERE a.tblx01_msg_metadata_msg_typ_x in ('Command','CommandResponse') AND a.tblx01_partition_date_x >= date_sub(current_date, {1}) \
	UNION \
	DISTINCT \
	SELECT DISTINCT \
	COALESCE(b.source,a.tblx01_msg_metadata_rgn_n) AS source \
	,a.tblx01_msg_metadata_msg_typ_x as msg_type \
	,a.tblx01_cloud_msg_d \
    ,a.tblx01_app_crltn_d_3 \
	,a.tblx01_wakeupsmsinvoked_r_3 AS wakeupsms_flag \
	,a.tblx01_vin_d_3 AS vin \
    ,a.tblx01_calcd_vin_1st_11_r AS vin_11 \
	,a.tblx01_esn_r AS esn \
	,a.tblx01_msg_metadata_msg_n AS cmdname \
	,a.tblx01_stat_c AS status \
	,a.tblx01_der_event_s_3 AS messagedate \
	,a.tblx01_msgmetadata_arrival_s AS arr_date \
	,a.tblx01_err_c AS cvrterrorcode \
	,a.tblx01_err_x AS errordetail \
	,a.tblx01_correlation_d AS correlationid \
	,a.tblx01_app_crltn_d_3 AS appcorrelationid \
	,b.activityid \
	,a.tblx01_raw_payload_metadata_cvrt_ver_r AS cvrt_version \
	,a.tblx01_pro_to_file_ver_x AS payload_cvrt_version \
	,a.tblx01_auth_stat_c AS auth_stat \
	,a.tblx01_anonymization_status_c_3 AS anon_flag \
	,a.tblx01_partition_region_x AS partition_region \
	,a.tblx01_partition_country_x AS partition_country \
	FROM {0}.Ntblx01_fcproc_MSG_RAWDATA_k2_PSU_SEC_HTE a lateral VIEW json_tuple(a.tblx01_msg_metadata_x_2, 'tag', 'ActivityID') b AS source,activityid \
	WHERE a.tblx01_msg_metadata_msg_typ_x in ('Command','CommandResponse') AND a.tblx01_partition_date_x >= date_sub(current_date, {1}) \
	".format(database,CRC4T_J9D_query_days))

	cmd_df = cmd_and_response_df.filter("msg_type = 'Command'").select(["source","vin","cmdname","messagedate","activityid","tblx01_cloud_msg_d","tblx01_app_crltn_d_3" \
	                                                                    ,"appcorrelationid","wakeupsms_flag","cvrt_version","partition_region","partition_country"]).dropDuplicates()

	response_df = cmd_and_response_df.filter("msg_type = 'CommandResponse'").select(["source","vin","esn","cmdname","status","messagedate","arr_date", \
	                                                                                 "cvrterrorcode","errordetail","correlationid","appcorrelationid", \
						                                         "activityid","cvrt_version","payload_cvrt_version","auth_stat", \
								                         "anon_flag","partition_region","partition_country"]).dropDuplicates()

	response_df2 = cmd_and_response_df.filter("msg_type = 'CommandResponse'").select(["source","vin","vin_11","esn","cmdname","status","messagedate","arr_date", \
                                                                                         "cvrterrorcode","errordetail","correlationid","appcorrelationid", \
                                                                                         "activityid","cvrt_version","payload_cvrt_version","auth_stat", \
                                                                                         "anon_flag","partition_region","partition_country"]).dropDuplicates()

	cmd_df.registerTempTable("ctbl")
	response_df.registerTempTable("crtbl")
	response_df2.registerTempTable("crtbl1")

	result = spark.sql("SELECT ccrfinal.vin,ccrfinal.esn,ccrfinal.correlationid,ccrfinal.appcorrelationid,ccrfinal.command_name,ccrfinal.command_timestamp \
        ,ccrfinal.command_status,ccrfinal.commandresponse_timestamp,ccrfinal.duration,ccrfinal.wakeupsms_flag,ccrfinal.cvrt_version,ccrfinal.authorization_status \
        ,ccrfinal.anonymization_flag,ccrfinal.cvrt_errorcode,ccrfinal.cvrt_errordetail,ccrfinal.command_activityid,ccrfinal.commandresponse_activityid,ccrfinal.source \
        ,ccrfinal.tcu_strategy_part_number,ccrfinal.command_pk,IF(COUNT(ccrfinal.command_pk) OVER(partition by ccrfinal.command_pk) >1, TRUE, FALSE) as suspect_flag \
        ,ccrfinal.partition_region,ccrfinal.partition_country,cast(to_date(COALESCE(ccrfinal.command_timestamp,ccrfinal.commandresponse_timestamp,cast('1990-01-01 00:00:00'as timestamp))) as string) as partition_date \
	FROM (SELECT ccrtbl.*,y.cvdcul_tcu_strat_part_no_c_3 as tcu_strategy_part_number,Sha2(Concat(COALESCE(ccrtbl.vin,'NULL'),COALESCE(esn,'NULL'),COALESCE(correlationid,'NULL') \
        ,COALESCE(appcorrelationid,'NULL'),COALESCE(command_name,'NULL'),COALESCE(Unix_timestamp(command_timestamp),Unix_timestamp(commandresponse_timestamp),0)),256) AS command_pk \
	FROM \
	( \
	SELECT DISTINCT \
	COALESCE(ctbl.vin,crtbl.vin,crtbl1.vin,crtbl1.vin_11) AS vin \
	,COALESCE(crtbl.esn,crtbl1.esn) AS esn \
	,COALESCE(ctbl.tblx01_cloud_msg_d,crtbl.correlationid,crtbl1.correlationid) AS correlationid \
	,COALESCE(crtbl.appcorrelationid,crtbl1.appcorrelationid,ctbl.tblx01_app_crltn_d_3) AS appcorrelationid \
	,COALESCE(ctbl.cmdname,crtbl.cmdname,crtbl1.cmdname) AS command_name \
	,ctbl.messagedate AS command_timestamp \
	,COALESCE(crtbl.status,crtbl1.status) AS command_status \
	,IF((COALESCE(crtbl.messagedate,crtbl1.messagedate) < ctbl.messagedate) OR (COALESCE(crtbl.messagedate,crtbl1.messagedate) > COALESCE(crtbl.arr_date,crtbl1.arr_date)) \
        ,COALESCE(crtbl.arr_date,crtbl1.arr_date), COALESCE(crtbl.messagedate,crtbl1.messagedate)) AS commandresponse_timestamp \
	,unix_timestamp(IF((COALESCE(crtbl.messagedate,crtbl1.messagedate) < ctbl.messagedate) OR (COALESCE(crtbl.messagedate,crtbl1.messagedate) > COALESCE(crtbl.arr_date,crtbl1.arr_date)) \
        ,COALESCE(crtbl.arr_date,crtbl1.arr_date),COALESCE(crtbl.messagedate,crtbl1.messagedate)))-unix_timestamp(ctbl.messagedate) AS duration \
	,ctbl.wakeupsms_flag AS wakeupsms_flag \
	,COALESCE(crtbl.payload_cvrt_version,crtbl.cvrt_version,crtbl1.payload_cvrt_version,crtbl1.cvrt_version,ctbl.cvrt_version) AS cvrt_version \
	,COALESCE(crtbl.auth_stat,crtbl1.auth_stat) AS authorization_status \
	,COALESCE(crtbl.anon_flag,crtbl1.anon_flag) AS anonymization_flag \
	,COALESCE(crtbl.cvrterrorcode,crtbl1.cvrterrorcode) AS cvrt_errorcode \
	,COALESCE(crtbl.errordetail,crtbl1.errordetail) AS cvrt_errordetail \
	,ctbl.activityid AS command_activityid \
	,COALESCE(crtbl.activityid,crtbl1.activityid) AS commandresponse_activityid \
	,COALESCE(ctbl.source,crtbl.source,crtbl1.source) AS source \
	,COALESCE(crtbl.partition_region,crtbl1.partition_region,ctbl.partition_region) AS partition_region \
	,COALESCE(crtbl.partition_country,crtbl1.partition_country,ctbl.partition_country) AS partition_country \
	FROM ctbl full outer join crtbl ON ctbl.vin=crtbl.vin \
        AND ctbl.tblx01_cloud_msg_d = crtbl.correlationid \
        AND ctbl.cmdname=crtbl.cmdname \
        AND ctbl.tblx01_cloud_msg_d IS NOT NULL \
        AND ctbl.vin IS NOT NULL \
        AND ctbl.cmdname IS NOT NULL \
        full outer join crtbl1 on ctbl.tblx01_app_crltn_d_3=crtbl1.appcorrelationid \
        AND ctbl.cmdname=crtbl1.cmdname \
        AND ctbl.tblx01_app_crltn_d_3 IS NOT NULL \
        AND ctbl.cmdname IS NOT NULL \
	) ccrtbl \
	LEFT OUTER JOIN {0}.NCVDCUL_PACE_TCU_STRAT_PART_NO_SEC_HTE y \
	ON ccrtbl.vin=y.cvdcul_vin_d_3 \
	WHERE (COALESCE(commandresponse_timestamp,command_timestamp) >= y.cvdcul_first_assoc_s_3 AND COALESCE(commandresponse_timestamp,command_timestamp) <= y.cvdcul_last_assoc_s_3) \
	OR COALESCE(commandresponse_timestamp,command_timestamp) IS NULL \
	OR y.cvdcul_vin_d_3 IS NULL \
	) ccrfinal where ccrfinal.vin IS NOT NULL".format(database))

	result.repartition("partition_region","partition_country","partition_date").filter("vin is not NULL") \
	      .write.partitionBy("partition_region","partition_country","partition_date").option("compression", "zlib").mode("overwrite") \
		  .orc(output)