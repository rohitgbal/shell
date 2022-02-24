use ${hivevar:db_name};
set hive.mapred.mode=nonstrict;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set tez.am.container.reuse.enabled=false;
set tez.am.resource.memory.mb=8192;
set hive.tez.container.size=8192;
set hive.tez.java.opts=-server -Xmx6144m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.orc.split.strategy=BI;


insert into table ${hivevar:monitoring_master}
select cvdsvt_msg_metadata_msg_n,"${currentUser}" as cvdcvt_created_by_c,FROM_UNIXTIME(UNIX_TIMESTAMP()) as cvdcvt_created_on_s from
(select cvdsvt_msg_metadata_msg_n,cvdcvt_msg_metadata_msg_n from  ${hivevar:monitoring_staging} stg_table
left outer join ${hivevar:monitoring_master} AXT
on stg_table.cvdsvt_msg_metadata_msg_n = AXT.cvdcvt_msg_metadata_msg_n)a
where a.cvdcvt_msg_metadata_msg_n is null
