use ${hivevar:db_name};

with qry as(select count(tblx01_vin_d_3) as cnt from NSCVP95_tcx_SCHED_HIST_PJ1_PII_HTE where to_date(tblx01_created_on_s)  = current_date)
insert into nprg2_workflow_reccalc_adm_hte partition(prg2_file_typ_c="${fileType}")
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
       "${masterTableNm}" as prg2_mstr_tbl_n,
       NULL as prg2_arch_diry_fldr_n,
       NULL as prg2_block_rec_t,
	   "${sourceTableNm}" as prg2_src_tbl_n
       from qry;