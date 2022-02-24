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


--Delete OLD/UNKNOWN data
DELETE FROM ${hivevar:mstr_na_usa_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_na_usa_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_status_x = 'OLD';


DELETE FROM ${hivevar:mstr_na_non_usa_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_na_non_usa_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x = 'CAN'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_na_non_usa_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_na_non_usa_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x != 'CAN'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_eu_gbr_fra_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_eu_gbr_fra_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x = 'GBR'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_eu_gbr_fra_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_eu_gbr_fra_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x = 'FRA'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_eu_non_gbr_fra_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x in('AUT','BEL','DEU','DNK','ESP','NLD','NOR','POL','PRT','SWE')
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_eu_non_gbr_fra_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_eu_non_gbr_fra_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x not in('AUT','BEL','DEU','DNK','ESP','NLD','NOR','POL','PRT','SWE')
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_ap_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_ap_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x = 'CHN'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_ap_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_ap_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x = 'AUS'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_ap_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_ap_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x not in('CHN','AUS')
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_rw_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_rw_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x = 'UNKNOWN'
and scvp99_partition_status_x = 'OLD';

DELETE FROM ${hivevar:mstr_rw_table}
WHERE scvp99_vin_d_2 in
(select distinct a.scvp99_vin_d_2
from ${hivevar:mstr_rw_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvp99_partition_cntry_x != 'UNKNOWN'
and scvp99_partition_status_x = 'OLD';

MSCK REPAIR TABLE ${hivevar:mstr_na_usa_table};
MSCK REPAIR TABLE ${hivevar:mstr_na_non_usa_table};
MSCK REPAIR TABLE ${hivevar:mstr_eu_gbr_fra_table};
MSCK REPAIR TABLE ${hivevar:mstr_eu_non_gbr_fra_table};
MSCK REPAIR TABLE ${hivevar:mstr_ap_table};
MSCK REPAIR TABLE ${hivevar:mstr_rw_table};