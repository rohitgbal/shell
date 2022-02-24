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

--for non usa
insert into table ${hivevar:temp_stg_table}
select a.scvp99_vin_d_2 as vin,
    a.scvp99_icc_d_2 as iccid,
    a.scvp99_cvrt_ver_r_2 as cvrt_version,
    a.scvp99_esn_x_2 as esn,
    a.scvp99_enty_d_2 as entity_id,
    a.scvp99_enty_typ_x_2 as entity_type,
    a.scvp99_consent_stat_x_2 as consent_status,
    a.scvp99_consent_seq_id_2 as consent_sequence_id,
    a.scvp99_consent_strt_s_2 as consent_start_time,
    a.scvp99_pte_ver_r_2 as pte_version,
    a.scvp99_pte_s_2 as pte_timestamp,
    a.scvp99_ufm_ver_r_2 as ufm_version,
    a.scvp99_ufm_s_2 as ufm_timestamp,
    b.save_region_code as partition_region,
    b.save_dlr_cntry_cd as partition_country
 from (select scvp99_vin_d_2,
       scvp99_cvrt_ver_r_2,
       scvp99_icc_d_2,
       scvp99_esn_x_2,
       scvp99_enty_d_2,
       scvp99_enty_typ_x_2,
       scvp99_consent_stat_x_2,
       scvp99_consent_seq_id_2,
       scvp99_consent_strt_s_2,
       scvp99_pte_ver_r_2,
       scvp99_pte_s_2,
       scvp99_ufm_ver_r_2,
       scvp99_ufm_s_2,
       scvp99_partition_cntry_x,
       scvp99_partition_status_x
       from ${hivevar:mstr_na_non_usa_table}
       lateral view explode(scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2) a
       join ${hivevar:vin_meta_table} b
       on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
       where a.scvp99_partition_cntry_x = 'UNKNOWN'
       and a.scvp99_partition_status_x = 'OLD'
       and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> '')
       union
       select
       c.scvp99_vin_d_2,
       c.scvp99_cvrt_ver_r_2,
       c.scvp99_icc_d_2,
       c.scvp99_esn_x_2,
       c.scvp99_enty_d_2,
       c.scvp99_enty_typ_x_2,
       c.scvp99_consent_stat_x_2,
       scvp99_consent_seq_id_2 as scvp99_consent_seq_d_2,
       c.scvp99_consent_strt_s_2,
       c.scvp99_pte_ver_r_2,
       c.scvp99_pte_s_2,
       c.scvp99_ufm_ver_r_2,
       c.scvp99_ufm_s_2,
       c.scvp99_partition_region_x,
       c.scvp99_partition_cntry_x
       from ${hivevar:mstr_na_non_usa_table} c lateral view explode(c.scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2
       where c.scvp99_vin_d_2 in
       (select distinct a.scvp99_vin_d_2
       from ${hivevar:mstr_na_non_usa_table} a
       join ${hivevar:vin_meta_table} b
       on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
       where a.scvp99_partition_cntry_x = 'UNKNOWN'
       and a.scvp99_partition_status_x = 'OLD'
       and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> ''))
       and c.scvp99_partition_status_x = 'OLD'
       and c.scvp99_partition_cntry_x <> 'UNKNOWN';

 --for non eu
 insert into table ${hivevar:temp_stg_table}
 select a.scvp99_vin_d_2 as vin,
     a.scvp99_icc_d_2 as iccid,
     a.scvp99_cvrt_ver_r_2 as cvrt_version,
     a.scvp99_esn_x_2 as esn,
     a.scvp99_enty_d_2 as entity_id,
     a.scvp99_enty_typ_x_2 as entity_type,
     a.scvp99_consent_stat_x_2 as consent_status,
     a.scvp99_consent_seq_id_2 as consent_sequence_id,
     a.scvp99_consent_strt_s_2 as consent_start_time,
     a.scvp99_pte_ver_r_2 as pte_version,
     a.scvp99_pte_s_2 as pte_timestamp,
     a.scvp99_ufm_ver_r_2 as ufm_version,
     a.scvp99_ufm_s_2 as ufm_timestamp,
     b.save_region_code as partition_region,
     b.save_dlr_cntry_cd as partition_country
  from (select scvp99_vin_d_2,
        scvp99_cvrt_ver_r_2,
        scvp99_icc_d_2,
        scvp99_esn_x_2,
        scvp99_enty_d_2,
        scvp99_enty_typ_x_2,
        scvp99_consent_stat_x_2,
        scvp99_consent_seq_id_2,
        scvp99_consent_strt_s_2,
        scvp99_pte_ver_r_2,
        scvp99_pte_s_2,
        scvp99_ufm_ver_r_2,
        scvp99_ufm_s_2,
        scvp99_partition_cntry_x,
        scvp99_partition_status_x
        from ${hivevar:mstr_eu_gbr_fra_table}
        lateral view explode(scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2) a
        join ${hivevar:vin_meta_table} b
        on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
        where a.scvp99_partition_cntry_x = 'UNKNOWN'
        and a.scvp99_partition_status_x = 'OLD'
        and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> '')
        union
        select
        c.scvp99_vin_d_2,
        c.scvp99_cvrt_ver_r_2,
        c.scvp99_icc_d_2,
        c.scvp99_esn_x_2,
        c.scvp99_enty_d_2,
        c.scvp99_enty_typ_x_2,
        c.scvp99_consent_stat_x_2,
        scvp99_consent_seq_id_2 as scvp99_consent_seq_d_2,
        c.scvp99_consent_strt_s_2,
        c.scvp99_pte_ver_r_2,
        c.scvp99_pte_s_2,
        c.scvp99_ufm_ver_r_2,
        c.scvp99_ufm_s_2,
        c.scvp99_partition_region_x,
        c.scvp99_partition_cntry_x
        from ${hivevar:mstr_eu_gbr_fra_table} c lateral view explode(c.scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2
        where c.scvp99_vin_d_2 in
        (select distinct a.scvp99_vin_d_2
        from ${hivevar:mstr_eu_gbr_fra_table} a
        join ${hivevar:vin_meta_table} b
        on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
        where a.scvp99_partition_cntry_x = 'UNKNOWN'
        and a.scvp99_partition_status_x = 'OLD'
        and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> ''))
        and c.scvp99_partition_status_x = 'OLD'
        and c.scvp99_partition_cntry_x <> 'UNKNOWN';

  --for AP
   insert into table ${hivevar:temp_stg_table}
   select a.scvp99_vin_d_2 as vin,
       a.scvp99_icc_d_2 as iccid,
       a.scvp99_cvrt_ver_r_2 as cvrt_version,
       a.scvp99_esn_x_2 as esn,
       a.scvp99_enty_d_2 as entity_id,
       a.scvp99_enty_typ_x_2 as entity_type,
       a.scvp99_consent_stat_x_2 as consent_status,
       a.scvp99_consent_seq_id_2 as consent_sequence_id,
       a.scvp99_consent_strt_s_2 as consent_start_time,
       a.scvp99_pte_ver_r_2 as pte_version,
       a.scvp99_pte_s_2 as pte_timestamp,
       a.scvp99_ufm_ver_r_2 as ufm_version,
       a.scvp99_ufm_s_2 as ufm_timestamp,
       b.save_region_code as partition_region,
       b.save_dlr_cntry_cd as partition_country
    from (select scvp99_vin_d_2,
          scvp99_cvrt_ver_r_2,
          scvp99_icc_d_2,
          scvp99_esn_x_2,
          scvp99_enty_d_2,
          scvp99_enty_typ_x_2,
          scvp99_consent_stat_x_2,
          scvp99_consent_seq_id_2,
          scvp99_consent_strt_s_2,
          scvp99_pte_ver_r_2,
          scvp99_pte_s_2,
          scvp99_ufm_ver_r_2,
          scvp99_ufm_s_2,
          scvp99_partition_cntry_x,
          scvp99_partition_status_x
          from ${hivevar:mstr_ap_table}
          lateral view explode(scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2) a
          join ${hivevar:vin_meta_table} b
          on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
          where a.scvp99_partition_cntry_x = 'UNKNOWN'
          and a.scvp99_partition_status_x = 'OLD'
          and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> '')
          union
          select
          c.scvp99_vin_d_2,
          c.scvp99_cvrt_ver_r_2,
          c.scvp99_icc_d_2,
          c.scvp99_esn_x_2,
          c.scvp99_enty_d_2,
          c.scvp99_enty_typ_x_2,
          c.scvp99_consent_stat_x_2,
          scvp99_consent_seq_id_2 as scvp99_consent_seq_d_2,
          c.scvp99_consent_strt_s_2,
          c.scvp99_pte_ver_r_2,
          c.scvp99_pte_s_2,
          c.scvp99_ufm_ver_r_2,
          c.scvp99_ufm_s_2,
          c.scvp99_partition_region_x,
          c.scvp99_partition_cntry_x
          from ${hivevar:mstr_ap_table} c lateral view explode(c.scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2
          where c.scvp99_vin_d_2 in
          (select distinct a.scvp99_vin_d_2
          from ${hivevar:mstr_ap_table} a
          join ${hivevar:vin_meta_table} b
          on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
          where a.scvp99_partition_cntry_x = 'UNKNOWN'
          and a.scvp99_partition_status_x = 'OLD'
          and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> ''))
          and c.scvp99_partition_status_x = 'OLD'
          and c.scvp99_partition_cntry_x <> 'UNKNOWN';

  --for RW
   insert into table ${hivevar:temp_stg_table}
   select a.scvp99_vin_d_2 as vin,
       a.scvp99_icc_d_2 as iccid,
       a.scvp99_cvrt_ver_r_2 as cvrt_version,
       a.scvp99_esn_x_2 as esn,
       a.scvp99_enty_d_2 as entity_id,
       a.scvp99_enty_typ_x_2 as entity_type,
       a.scvp99_consent_stat_x_2 as consent_status,
       a.scvp99_consent_seq_id_2 as consent_sequence_id,
       a.scvp99_consent_strt_s_2 as consent_start_time,
       a.scvp99_pte_ver_r_2 as pte_version,
       a.scvp99_pte_s_2 as pte_timestamp,
       a.scvp99_ufm_ver_r_2 as ufm_version,
       a.scvp99_ufm_s_2 as ufm_timestamp,
       b.save_region_code as partition_region,
       b.save_dlr_cntry_cd as partition_country
    from (select scvp99_vin_d_2,
          scvp99_cvrt_ver_r_2,
          scvp99_icc_d_2,
          scvp99_esn_x_2,
          scvp99_enty_d_2,
          scvp99_enty_typ_x_2,
          scvp99_consent_stat_x_2,
          scvp99_consent_seq_id_2,
          scvp99_consent_strt_s_2,
          scvp99_pte_ver_r_2,
          scvp99_pte_s_2,
          scvp99_ufm_ver_r_2,
          scvp99_ufm_s_2,
          scvp99_partition_cntry_x,
          scvp99_partition_status_x
          from ${hivevar:mstr_rw_table}
          lateral view explode(scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2) a
          join ${hivevar:vin_meta_table} b
          on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
          where a.scvp99_partition_cntry_x = 'UNKNOWN'
          and a.scvp99_partition_status_x = 'OLD'
          and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> '')
          union
          select
          c.scvp99_vin_d_2,
          c.scvp99_cvrt_ver_r_2,
          c.scvp99_icc_d_2,
          c.scvp99_esn_x_2,
          c.scvp99_enty_d_2,
          c.scvp99_enty_typ_x_2,
          c.scvp99_consent_stat_x_2,
          scvp99_consent_seq_id_2 as scvp99_consent_seq_d_2,
          c.scvp99_consent_strt_s_2,
          c.scvp99_pte_ver_r_2,
          c.scvp99_pte_s_2,
          c.scvp99_ufm_ver_r_2,
          c.scvp99_ufm_s_2,
          c.scvp99_partition_region_x,
          c.scvp99_partition_cntry_x
          from ${hivevar:mstr_rw_table} c lateral view explode(c.scvp99_consent_seq_d_2) seqTable as scvp99_consent_seq_id_2
          where c.scvp99_vin_d_2 in
          (select distinct a.scvp99_vin_d_2
          from ${hivevar:mstr_rw_table} a
          join ${hivevar:vin_meta_table} b
          on LPAD(a.scvp99_vin_d_2,18,SUBSTR(a.scvp99_vin_d_2,-1)) = b.vin
          where a.scvp99_partition_cntry_x = 'UNKNOWN'
          and a.scvp99_partition_status_x = 'OLD'
          and (b.save_dlr_cntry_cd <> 'UNK' and b.save_dlr_cntry_cd is not null and trim(b.save_dlr_cntry_cd) <> ''))
          and c.scvp99_partition_status_x = 'OLD'
          and c.scvp99_partition_cntry_x <> 'UNKNOWN';