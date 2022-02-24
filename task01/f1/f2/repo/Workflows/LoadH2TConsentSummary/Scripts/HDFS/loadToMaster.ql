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

INSERT INTO TABLE ${mstr_table}
PARTITION
(scvp99_partition_cntry_x, scvp99_partition_date_x, scvp99_partition_status_x)
SELECT * from ${hivevar:stg_table} WHERE
CASE
WHEN ${region} = 'NA' and ${country} = 'USA' THEN partition_region = 'NA' and partition_cntry = 'USA'
WHEN ${region} = 'NA' and ${country} = 'Non_USA' THEN partition_region = 'NA' and partition_cntry != 'USA'
WHEN ${region} = 'EU' and ${country} = 'GBR_FRA' THEN partition_region = 'EU' and partition_cntry in('GBR','FRA')
WHEN ${region} = 'EU' and ${country} = 'Non_GBR_FRA' THEN partition_region = 'EU' and partition_cntry not in('GBR','FRA')
WHEN ${region} = 'AP' and ${country} = '' THEN partition_region = 'AP'
ELSE partition_region not in('NA','EU','AP')
END;