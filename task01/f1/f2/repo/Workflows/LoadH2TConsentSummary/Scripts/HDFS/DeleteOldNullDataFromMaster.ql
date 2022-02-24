use ${hivevar:db_name};

set hive.stats.autogather=false;
set hive.exec.reducers.bytes.per.reducer=10432;
set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=ETL;
set hive.orc.cache.use.soft.references=true;
set hive.tez.java.opts=-Xmx32768m;
set hive.tez.container.size=32768;
set tez.am.resource.memory.mb=16384;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;
set hive.msck.path.validation=ignore;


DELETE FROM ${hivevar:mstr_na_usa_table}
WHERE scvp99_partition_cntry_x ='USA'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_na_non_usa_table}
WHERE scvp99_partition_cntry_x ='CAN'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_na_non_usa_table}
WHERE scvp99_partition_cntry_x ='MEX'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_na_non_usa_table}
WHERE scvp99_partition_cntry_x not in('CAN','MEX')
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'GBR'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'FRA'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'AUT'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'BEL'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'DEU'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'DNK'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'ESP'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'NLD'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'NOR'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'POL'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'PRT'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x = 'SWE'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_partition_cntry_x not in('AUT','BEL','DEU','DNK','ESP','NLD','NOR','POL','PRT','SWE')
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_ap_table}
WHERE scvp99_partition_cntry_x = 'CHN'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_ap_table}
WHERE scvp99_partition_cntry_x = 'AUS'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_ap_table}
WHERE scvp99_partition_cntry_x not in('CHN','AUS')
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_rw_table}
WHERE scvp99_partition_cntry_x = 'UNKNOWN'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

DELETE FROM ${hivevar:mstr_rw_table}
WHERE scvp99_partition_cntry_x != 'UNKNOWN'
and scvp99_rec_stat_x_2 = 'OLD'
AND scvp99_created_by_c is null
and scvp99_created_on_s is null;

MSCK REPAIR TABLE ${hivevar:mstr_na_usa_table};
MSCK REPAIR TABLE ${hivevar:mstr_na_non_usa_table};
MSCK REPAIR TABLE ${hivevar:mstr_eu_gbr_fra_table};
MSCK REPAIR TABLE ${hivevar:mstr_eu_non_gbr_fra_table};
MSCK REPAIR TABLE ${hivevar:mstr_ap_table};
MSCK REPAIR TABLE ${hivevar:mstr_rw_table};