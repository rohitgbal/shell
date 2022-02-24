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

insert into table ${hivevar:temp_stg_table}
select a.vin,
a.cvrt_version,
a.iccid,
a.esn,
a.entity_id,
a.entity_type,
a.consent_status,
a.consent_sequence_id,
a.consent_start_time,
a.pte_version,
a.pte_timestamp,
a.ufm_version,
a.ufm_timestamp,
if(b.save_region_code ='UNK' or b.save_region_code is null or trim(b.save_region_code) = '' or b.save_dlr_cntry_cd ='UNK' or b.save_dlr_cntry_cd is null or trim(b.save_dlr_cntry_cd) = '', 'UNKNOWN', b.save_region_code ) as partition_region,
if(b.save_dlr_cntry_cd ='UNK' or b.save_dlr_cntry_cd is null or trim(b.save_dlr_cntry_cd) = '', 'UNKNOWN', b.save_dlr_cntry_cd) as partition_country
from
    (select tblx01_vin_d_3 as vin,
    coalesce(tblx01_pro_to_file_ver_x,tblx01_raw_payload_metadata_cvrt_ver_r) as cvrt_version,
    tblx01_icc_d_2 as iccid,
    tblx01_esn_r as esn,
    sttg.entityId as entity_id,
    sttg.entityType as entity_type,
    sttg.optIn as consent_status,
    sttg.consentSequenceId as consent_sequence_id,
    case when (sttg.consenttimestamp > tblx01_der_event_s_3) then tblx01_der_event_s_3 when to_date(sttg.consenttimestamp) < date_sub('${current_date}',${lkb_days}) then tblx01_der_event_s_3 else sttg.consenttimestamp end as consent_start_time,
    case when isnull(tblx01_policytableextensiontimestamp_s_3) then null else concat(tblx01_policytableextension_platformversion_r_3,'.',tblx01_policytableextension_majorversion_r_3,'.',tblx01_policytableextension_minorversion_r_3) end as pte_version,
    tblx01_policytableextensiontimestamp_s_3 as pte_timestamp,
    case when isnull(tblx01_userfriendlymessagestimestamp_s_3) then null else concat(tblx01_userfriendlymessages_platformversion_r_3,'.',tblx01_userfriendlymessages_majorversion_r_3,'.',tblx01_userfriendlymessages_minorversion_r_3) end as ufm_version,
    tblx01_userfriendlymessagestimestamp_s_3 as ufm_timestamp
    from ${hivevar:raw_table}
    lateral view explode(tblx01_entity_settings_x_3) es as sttg
    where tblx01_partition_date_x >= date_sub('${current_date}',${lkb_days})
    and tblx01_partition_date_x < '${current_date}'
    and lower(tblx01_msg_metadata_msg_typ_x) in (lower('Alert'), lower('QueryResponse'))
    and lower(tblx01_msg_metadata_msg_n) like lower('%H2T%')
    and sttg.entityId is not null
    and sttg.optIn is not null
    and sttg.consenttimestamp is not null
    ) a
    left join ${hivevar:vin_meta_table} b
    on LPAD(a.vin,18,SUBSTR(a.vin,-1)) = b.vin;
