use ${hivevar:db_name};

MSCK REPAIR TABLE ${hivevar:temp_stg_table};