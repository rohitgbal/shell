use ${hivevar:db_name};

alter table ${hivevar:stg_table_name} drop if exists partition(cvdsvt_partition_cntry_x >= '');