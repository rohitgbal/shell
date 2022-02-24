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
DYS=${12}
SOURCE_TABLE=${13}
CURRENT_DATE=${14}
START_DATE=${15}
ASSEMBLY_JAR=${16}
HIVE_JDBC_URL=${17}


#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

export SPARK_MAJOR_VERSION=2
spark-submit --master $SPARK_MASTER \
             --deploy-mode $SPARK_DEPLOY_MODE \
             --conf spark.dynamicAllocation.maxExecutors=$SPARK_MAX_EXECUTORS \
			 --conf spark.hadoop.metastore.catalog.default=hive \
             --driver-memory $SPARK_DRIVER_MEMORY \
             --executor-memory $SPARK_EXECUTOR_MEMORY \
			 --executor-cores $SPARK_EXECUTOR_CORES \
			 --conf spark.sql.shuffle.partitions=1000 \
			 --jars $ASSEMBLY_JAR \
			 --py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc*.zip \
			 --conf spark.sql.hive.hiveserver2.jdbc.url=$HIVE_JDBC_URL \
             --conf spark.sql.hive.hiveserver2.jdbc.url.principal="hive/XXXX@XXXX.COM" \
             --conf spark.datasource.hive.warehouse.load.staging.dir=/tmp/staging/hwc \
             --conf spark.security.credentials.hiveserver2.enabled=true \
             --conf spark.datasource.hive.warehouse.read.mode=staging_output \
             --conf spark.sql.hive.conf.list="hive.support.quoted.identifiers=none" \
             --conf spark.port.maxRetries=250 \
			 --files LgsDetailsReloadTransform.sql \
             LgsDetailsReloadTransform.py --database $DATABASE --sql LgsDetailsReloadTransform.sql --user $USER --source_table $SOURCE_TABLE --output $OUTPUT --lookbacktable $DYS --current_date $CURRENT_DATE --start_date $START_DATE

																																													
if [ "$?" != "0" ]; then
	 echo "*** LgsDetails Reload Failed"
   exit 1
fi
exit 0																																													
