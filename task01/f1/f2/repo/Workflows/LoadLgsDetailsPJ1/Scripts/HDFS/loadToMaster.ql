use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=BI;
set hive.orc.cache.use.soft.references=true;
set hive.tez.java.opts=-Xmx24576m;
set hive.tez.container.size=32768;
set tez.am.resource.memory.mb=16384;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;



msck repair table NCVDSX8_WIL_SUMM_PJ1_ST_HTE;

INSERT OVERWRITE TABLE NCVDCX8_WIL_SUMM_PJ1_SEC_HTE
PARTITION
(cvdcx8_partition_cntry_x, cvdcx8_partition_date_x)
SELECT * from NCVDSX8_WIL_SUMM_PJ1_ST_HTE;

msck repair table NCVDCX8_WIL_SUMM_PJ1_SEC_HTE;


