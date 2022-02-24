use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=BI;
set hive.orc.cache.use.soft.references=true;
set hive.tez.java.opts=-Xmx24576m;
set hive.tez.container.size=32768;
set tez.am.resource.memory.mb=16384;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;
set hive.msck.path.validation=ignore;


--Delete OLD/UNKNOWN data
DELETE FROM ${hivevar:mstr_table}
WHERE scvpbb_vin_d_2 in
(select distinct a.scvpbb_vin_d_2
from ${hivevar:mstr_table} a
join ${hivevar:vin_meta_table} b
on LPAD(a.scvpbb_vin_d_2,18,SUBSTR(a.scvpbb_vin_d_2,-1)) = b.vin
and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(save_dlr_cntry_cd) <> ''))
and scvpbb_partition_cntry_x = 'UNKNOWN'
and scvpbb_partition_status_x = 'OLD';


MSCK REPAIR TABLE ${hivevar:mstr_table};