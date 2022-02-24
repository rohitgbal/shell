use ${hivevar:db_name};

alter table Nprg2_WORKFLOW_reccalc_ADM_HTE add partition(prg2_file_typ_c='dataprogRepository-J9D-PJ1');
