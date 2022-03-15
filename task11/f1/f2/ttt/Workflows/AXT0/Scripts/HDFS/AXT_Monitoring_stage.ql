use ${hivevar:db_name};

Msck repair table ${hivevar:stg_table};

insert into table ${hivevar:monitoring_staging}
select 
distinct cvdsvt_msg_metadata_msg_n
from ${hivevar:stg_table};

