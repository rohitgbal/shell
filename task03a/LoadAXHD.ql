use ${hivevar:db_name};

-------------------------------FeatureEnrollment LIST------------------------------------------------------

create external table if not exists NjcdtGH_AXHD_FTR_ENRL_LST_PII_HTE  
(
   jcdtgh_sha_k string,
   jcdtgh_vin_d_2 Varchar(17),
   jcdtgh_vin_hash_x_2 Varchar(100),
   jcdtgh_enrl_stat_r_2 int,
   jcdtgh_pkg_c_2 Varchar(25),
   jcdtgh_trm_strt_date_s_2 Timestamp,
   jcdtgh_authz_usr_x_2 Varchar(100),
   jcdtgh_auth_s_2 Timestamp,
   jcdtgh_loc_r_2 int,
   jcdtgh_veh_connectivity_r_2 int,
   jcdtgh_veh_dta_r_2 int,
   jcdtgh_crt_date_s_2 Timestamp,
   jcdtgh_updt_date_s_2 Timestamp,
   jcdtgh_raw_payload_x_2 string,
   jcdtgh_proc_stat_c string,
   jcdtgh_proc_stat_dtl_x string,
   jcdtgh_proc_stat_utc_s timestamp,
   jcdtgh_created_on_s timestamp,
   jcdtgh_created_by_c varchar(10)
)
partitioned by (njcdtgh_partition_date_time_key_creation_x string, njcdtgh_partition_region_c string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/barred/AXHD/History/njcdtgh_AXHD_ftr_enrl_lst_pii_hte'
tblproperties ('orc.compress.size'='8192',xxxx);

create external table if not exists rgceSGH_AXHD_FTR_ENRL_LST_ST_HTE  
(
   sha_key string,
   vin Varchar(17),
   vin_hash Varchar(100),
   enrollment_status int,
   package_code Varchar(25),
   term_start_date Timestamp,
   authorized_user Varchar(100),
   authorization_timestamp Timestamp,
   location int,
   vehicle_connectivity int,
   vehicle_data int,
   created_date Timestamp,
   updated_date Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcesgh_AXHD_ftr_enrl_lst_st_hte';

create external table if not exists rgceZGH_AXHD_FTR_ENRL_LST_NONDUP_ST_HTE  
(
   sha_key string,
   vin Varchar(17),
   vin_hash Varchar(100),
   enrollment_status int,
   package_code Varchar(25),
   term_start_date Timestamp,
   authorized_user Varchar(100),
   authorization_timestamp Timestamp,
   location int,
   vehicle_connectivity int,
   vehicle_data int,
   created_date Timestamp,
   updated_date Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp,
   r_num int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcezgh_AXHD_ftr_enrl_lst_nondup_st_hte';

create external table if not exists rgceDGH_AXHD_FTR_ENRL_LST_DUP_HTE  
(
   scvdgh_sha_k string,
   scvdgh_created_on_s TIMESTAMP,
   scvdgh_created_by_c VARCHAR(10)
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Public/AXHD/History/rgcedgh_AXHD_ftr_enrl_lst_dup_hte';

create external table if not exists rgceEGH_AXHD_FTR_ENRL_LST_TEMP_DUP_ST_HTE  
(
   scvegh_sha_k string
)
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgceegh_AXHD_ftr_enrl_lst_temp_dup_st_hte';


-------------------------------Key LIST--------------------------------------------------------------------

create external table if not exists NjcdtGI_AXHD_KEY_LST_PII_HTE  
(
   jcdtgi_sha_k string,
   jcdtgi_vin_d_2 Varchar(17),
   jcdtgi_key_st_x_2 Varchar(50),
   jcdtgi_key_d_2 Varchar(50),
   jcdtgi_guid_x_2 Varchar(75),
   jcdtgi_muid_x_2 Varchar(75),
   jcdtgi_date_time_key_creatn_s_2 Timestamp,
   jcdtgi_raw_payload_x_2 string,
   jcdtgi_proc_stat_c string,
   jcdtgi_proc_stat_dtl_x string,
   jcdtgi_proc_stat_utc_s timestamp,
   jcdtgi_created_on_s timestamp,
   jcdtgi_created_by_c varchar(10)
)
partitioned by (njcdtgi_partition_date_time_key_creation_x string, njcdtgi_partition_region_c string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/barred/AXHD/History/njcdtgi_AXHD_key_lst_pii_hte'
tblproperties ('orc.compress.size'='8192');

create external table if not exists rgceSGI_AXHD_KEY_LST_ST_HTE  
(
   sha_key string,
   vin Varchar(17),
   key_state Varchar(50),
   key_id Varchar(50),
   guid Varchar(75),
   muid Varchar(75),
   date_time_key_creation Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcesgi_AXHD_key_lst_st_hte';

create external table if not exists rgceZGI_AXHD_KEY_LST_NONDUP_ST_HTE  
(
   sha_key string,
   vin Varchar(17),
   key_state Varchar(50),
   key_id Varchar(50),
   guid Varchar(75),
   muid Varchar(75),
   date_time_key_creation Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp,
   r_num int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcezgi_AXHD_key_lst_nondup_st_hte';

create external table if not exists rgceDGI_AXHD_KEY_LST_DUP_HTE  
(
   scvdgi_sha_k string,
   scvdgi_created_on_s TIMESTAMP,
   scvdgi_created_by_c VARCHAR(10)
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Public/AXHD/History/rgcedgi_AXHD_key_lst_dup_hte';

create external table if not exists rgceEGI_AXHD_KEY_LST_TEMP_DUP_ST_HTE  
(
   scvegi_sha_k string
)
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgceegi_AXHD_key_lst_temp_dup_st_hte';


-------------------------------Mobile Key LIST------------------------------------------------------

create external table if not exists NjcdtGJ_AXHD_MOB_KEY_LST_PII_HTE  
(
   jcdtgj_sha_k string,
   jcdtgj_muid_hash_x_2 Varchar(75),
   jcdtgj_muid_x_2 Varchar(75),
   jcdtgj_guid_x_2 Varchar(75),
   jcdtgj_make_x_2 Varchar(50),
   jcdtgj_raw_mdl_x_2 Varchar(75),
   jcdtgj_simplified_mdl_x_2 Varchar(75),
   jcdtgj_os_ver_x_2 Varchar(30),
   jcdtgj_crt_date_s_2 Timestamp,
   jcdtgj_raw_payload_x_2 string,
   jcdtgj_proc_stat_c string,
   jcdtgj_proc_stat_dtl_x string,
   jcdtgj_proc_stat_utc_s timestamp,
   jcdtgj_created_on_s timestamp,
   jcdtgj_created_by_c varchar(10)
)
partitioned by (njcdtgj_partition_date_time_key_creation_x string, njcdtgj_partition_region_c string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/barred/AXHD/History/njcdtgj_AXHD_mob_key_lst_pii_hte'
tblproperties ('orc.compress.size'='8192');

create external table if not exists rgceSGJ_AXHD_MOB_KEY_LST_ST_HTE  
(
   sha_key string,
   muid_hash Varchar(75),
   muid Varchar(75),
   guid Varchar(75),
   make Varchar(50),
   raw_model Varchar(75),
   simplified_model Varchar(75),
   os_version Varchar(30),
   created_date Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcesgj_AXHD_mob_key_lst_st_hte';

create external table if not exists rgceZGJ_AXHD_MOB_KEY_LST_NONDUP_ST_HTE  
(
   sha_key string,
   muid_hash Varchar(75),
   muid Varchar(75),
   guid Varchar(75),
   make Varchar(50),
   raw_model Varchar(75),
   simplified_model Varchar(75),
   os_version Varchar(30),
   created_date Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp,
   r_num int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcezgj_AXHD_mob_key_lst_nondup_st_hte';

create external table if not exists rgceDGJ_AXHD_MOB_KEY_LST_DUP_HTE  
(
   scvdgj_sha_k string,
   scvdgj_created_on_s TIMESTAMP,
   scvdgj_created_by_c VARCHAR(10)
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Public/AXHD/History/rgcedgj_AXHD_mob_key_lst_dup_hte';

create external table if not exists rgceEGJ_AXHD_MOB_KEY_LST_TEMP_DUP_ST_HTE  
(
   scvegj_sha_k string
)
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgceegj_AXHD_mob_key_lst_temp_dup_st_hte';

-------------------------------PnC LIST--------------------------------------------------------------------

create external table if not exists NjcdtGQ_AXHD_PNCLIST_PII_HTE  
(
   jcdtgq_sha_k string,
   jcdtgq_vin_d_2 Varchar (17),
   jcdtgq_usr_guid_x_2 Varchar (100),
   jcdtgq_stat_c_2 Varchar (100),
   jcdtgq_emaid_hash_c_2 Varchar (200),
   jcdtgq_certif_d_2 Varchar (100),
   jcdtgq_certif_typ_x_2 Varchar (50),
   jcdtgq_certif_rgn_c_2 Varchar (20),
   jcdtgq_certif_prvd_x_2 Varchar (20),
   jcdtgq_expiry_date_s_2 Timestamp,
   jcdtgq_raw_payload_x_2 string,
   jcdtgq_proc_stat_c string,
   jcdtgq_proc_stat_dtl_x string,
   jcdtgq_proc_stat_utc_s timestamp,
   jcdtgq_created_on_s timestamp,
   jcdtgq_created_by_c varchar(10)
)
partitioned by (jcdtgq_partition_date_time_key_creation_x string, jcdtgq_partition_region_c string)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/barred/AXHD/History/njcdtgq_AXHD_pnclist_pii_hte'
tblproperties ('orc.compress.size'='8192');

create external table if not exists rgceSGQ_AXHD_PNCLIST_ST_HTE  
(
   sha_key string,
   vin Varchar (17),
   user_guid Varchar (100),
   status_code Varchar (100),
   emaid_hash Varchar (200),
   certificate_id Varchar (100),
   certificate_type Varchar (50),
   certificate_region Varchar (20),
   certificate_provider Varchar (20),
   expiry_date Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcesgq_AXHD_pnclist_st_hte';

create external table if not exists rgceZGQ_AXHD_PNCLIST_NONDUP_ST_HTE  
(
   sha_key string,
   vin Varchar (17),
   user_guid Varchar (100),
   status_code Varchar (100),
   emaid_hash Varchar (200),
   certificate_id Varchar (100),
   certificate_type Varchar (50),
   certificate_region Varchar (20),
   certificate_provider Varchar (20),
   expiry_date Timestamp,
   raw_payload string,
   partition_date_time_key_creation string,
   partition_region string,
   process_status string,
   process_status_details string,
   process_status_date_time_utc timestamp,
   r_num int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgcezgq_AXHD_pnclist_nondup_st_hte';

create external table if not exists rgceDGQ_AXHD_PNCLIST_DUP_HTE  
(
   scvdgq_sha_k string,
   scvdgq_created_on_s TIMESTAMP,
   scvdgq_created_by_c VARCHAR(10)
)
stored as orc
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Public/AXHD/History/rgcedgq_AXHD_pnclist_dup_hte';

create external table if not exists rgceEGQ_AXHD_PNCLIST_TEMP_DUP_ST_HTE  
(
   scvegq_sha_k string
)
location '/project/prg1/prg1${hivevar:hdfs_env}/buckst/Staging/AXHD/History/rgceegq_AXHD_pnclist_temp_dup_st_hte';
