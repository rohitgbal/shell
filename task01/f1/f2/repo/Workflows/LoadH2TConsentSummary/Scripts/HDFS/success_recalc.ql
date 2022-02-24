use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000000;
set hive.optimize.sort.dynamic.partition=true;
set hive.exec.parallel=true;
SET hive.tez.container.size=10240;
set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
SET hive.exec.orc.split.strategy=BI;
set tez.runtime.io.sort.mb=1024;

msck repair table ${stg_table};

with qry as (select count(partition_date) as cnt from ${stg_table})
insert into ${reccalc_table} partition(prg2_file_typ_c="${reccalc_file_type}")
select "${workflowId}" as prg2_workflow_d,
       "${workflowName}" as prg2_workflow_n,
       "Success" as prg2_workflow_stat_c,
       NULL as prg2_workflow_falur_step_x,
       cnt as prg2_inp_rec_t,
       cnt as prg2_trans_good_rec_t,
       NULL as prg2_trans_fail_rec_t,
       NULL as prg2_bad_rec_t,
       NULL as prg2_dup_rec_t,
       to_date(from_unixtime(unix_timestamp())) as prg2_procd_y,
       from_unixtime(unix_timestamp()) as prg2_proc_strt_s,
       from_unixtime(unix_timestamp()) as prg2_proc_end_s,
       from_unixtime(unix_timestamp()) as prg2_last_updt_on_s,
       "${current_user}" as prg2_last_updt_by_c,
       "${mstr_na_usa_table},${mstr_na_non_usa_table},${mstr_eu_gbr_fra_table},${mstr_eu_non_gbr_fra_table},mstr_ap_table,mstr_rw_table" as prg2_mstr_tbl_n,
       NULL as prg2_arch_diry_fldr_n,
       NULL as prg2_block_rec_t,
       "${raw_k2_table},${raw_k2_prs_table},${raw_k2_psa_table},${raw_k2_psu_table}" as prg2_src_tbl_n from qry;