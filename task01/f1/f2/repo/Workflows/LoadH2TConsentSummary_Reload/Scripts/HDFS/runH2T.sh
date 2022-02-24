#!/bin/bash

SPARK_MASTER=${1}
SPARK_DEPLOY_MODE=${2}
SPARK_MAX_EXECUTORS=${3}
SPARK_DRIVER_MEMORY=${4}
SPARK_EXECUTOR_MEMORY=${5}
SPARK_EXECUTOR_CORES=${6}
OUTPUT=${7}
DATABASE=${8}
KERBEROS_KEYTAB_FILENAME=${9}
KERBEROS_PRINCIPAL=${10}
USER=${11}
CURRENT_DATE=${12}
DYS=${13}
HISTORY_LOAD=${14}
TEMP_STG_TABLE=${15}


#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

export SPARK_MAJOR_VERSION=2
spark-submit --master $SPARK_MASTER \
      --deploy-mode $SPARK_DEPLOY_MODE \
      --conf spark.dynamicAllocation.maxExecutors=$SPARK_MAX_EXECUTORS \
			--conf spark.hadoop.metastore.catalog.default=hive \
			--conf spark.yarn.executor.memoryOverhead=6g \
			--conf spark.driver.maxResultSize=6g \
			--conf spark.sql.hive.convertMetastoreOrc='true' \
		  --conf spark.sql.orc.char.enabled='true' \
		  --conf spark.sql.orc.filterPushdown='true' \
			--conf spark.sql.orc.enabled='true' \
			--conf spark.network.timeout=1200 \
			--conf spark.sql.broadcastTimeout=1000 \
			--conf spark.sql.tungsten.enabled='false' \
			--conf spark.shuffle.spillAfterRead='true' \
			--conf spark.executor.extraJavaOptions='-XX:+UseNUMA -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12 -XX:+UseStringDeduplication' \
			--conf spark.default.parallelism=500 \
			--conf spark.driver.extraJavaOptions=-Duser.timezone="UTC" \
			--conf spark.yarn.max.executor.failures=200 \
			--conf spark.task.maxFailures=8 \
			--conf spark.dynamicAllocation.executorIdleTimeout=1000 \
			--driver-memory $SPARK_DRIVER_MEMORY \
      --executor-memory $SPARK_EXECUTOR_MEMORY \
			--executor-cores $SPARK_EXECUTOR_CORES \
			--conf spark.sql.shuffle.partitions=5000 \
			--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
		  --conf spark.datasource.hive.warehouse.load.staging.dir=/tmp/staging/hwc \
		  --conf spark.security.credentials.hiveserver2.enabled=true \
		  --conf spark.datasource.hive.warehouse.read.mode=staging_output \
		  --conf spark.sql.hive.conf.list="hive.support.quoted.identifiers=none" \
			--jars /s/hadoop/user/jars/hive-warehouse-connector-assembly-NNDhdp2.jar \
			--py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-* \
			--jars /opt/cloudera/parcels/CDH-7.1.6-1.cdh7.1.6.p6.12486751/jars/hive-warehouse-connector-assembly-1.0.0.7.1.6.6-5.jar \
		  --py-files /opt/cloudera/parcels/CDH-7.1.6-1.cdh7.1.6.p6.12486751/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.6.6-5.zip \
		  --conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
		  --conf spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator \
		  --conf spark.datasource.hive.warehouse.read.mode=DIRECT_READER_V1 \
			 H2Ttransform.py --database $DATABASE --user $USER --tempstgtable $TEMP_STG_TABLE --lkbdays $DYS --currdate $CURRENT_DATE --historyload $HISTORY_LOAD --output $OUTPUT


if [ "$?" != "0" ]; then
	 echo "*** H2T 4G Historical Load Failed"
   exit 1
fi
exit 0