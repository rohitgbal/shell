use ${hivevar:db_name};

alter table ncvdsvt_AXT_summary_pj1_st_hte drop if exists partition(cvdsvt_partition_cntry_x >= '');