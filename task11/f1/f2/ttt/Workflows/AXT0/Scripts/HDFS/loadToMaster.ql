use ${hivevar:db_name};

set hive.execution.engine=tez;
set hive.exec.orc.split.strategy=BI;
set hive.orc.cache.use.soft.references=true;
--set hive.tez.java.opts=-Xmx24576m;
set hive.tez.container.size=24576;
set tez.am.resource.memory.mb=24576;
--set hive.tez.java.opts=-server -Xmx16384m -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps;
set hive.exec.max.dynamic.partitions=10000;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.optimize.sort.dynamic.partition=false;

alter table ${hivevar:stg_table_name} drop if exists partition(cvdsvt_partition_cntry_x > '');

msck repair table ${hivevar:stg_table_name};


INSERT OVERWRITE TABLE ${hivevar:mstr_table_name}
PARTITION
(cvdcvt_partition_cntry_x,cvdcvt_partition_date_x)
select cvdsvt_sha_k                            
,cvdsvt_sha_k_AXT                        
,cvdsvt_vin_d_3                                        
,cvdsvt_event_s_3                        
,cvdsvt_nump_r_2                         
,cvdsvt_lon_r_3                          
,cvdsvt_lat_r_3                          
,cvdsvt_ecu_d_2                          
,cvdsvt_AXT_d_2                          
,cvdsvt_AXT_info_byte_x_2                
,cvdsvt_AXT_status_byte_x_2              
,cvdsvt_test_fald_r_2                    
,cvdsvt_test_fald_this_oper_cyc_r_2      
,cvdsvt_pend_AXT_r_2                     
,cvdsvt_confmd_AXT_r_2                   
,cvdsvt_test_not_cmpltd_since_lst_clr_r_2
,cvdsvt_test_fald_since_lst_clr_r_2      
,cvdsvt_test_not_cmpltd_this_oper_cyc_r_2
,cvdsvt_wrng_ind_reqstd_r_2              
,cvdsvt_src_c                            
,cvdsvt_region_x                         
,cvdsvt_created_by_c                     
,cvdsvt_created_on_s                     
,cvdsvt_raw_payload_metadata_cvrt_ver_x  
,cvdsvt_auth_stat_c                      
,cvdsvt_lifecycmde_d_actl_r              
,cvdsvt_partition_cntry_x                
,cvdsvt_partition_date_x  
from ${hivevar:stg_table_name} where cvdsvt_partition_date_x >= DATE_SUB('${current_date}',${hivevar:lkb_days});
