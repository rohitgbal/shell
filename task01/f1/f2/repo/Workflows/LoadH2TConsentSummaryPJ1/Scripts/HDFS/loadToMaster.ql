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


DELETE FROM  NSCVPBB_H2T_SUMM_PJ1_PII_HTE WHERE scvpbb_partition_status_x='NEW';

MSCK REPAIR TABLE NSCVPBB_H2T_SUMM_PJ1_PII_HTE;

INSERT INTO TABLE NSCVPBB_H2T_SUMM_PJ1_PII_HTE
PARTITION
(scvpbb_partition_region_x, scvpbb_partition_cntry_x, scvpbb_partition_date_x, scvpbb_partition_status_x)
SELECT * from NSCVSBB_H2T_SUMM_PJ1_ST_HTE;



