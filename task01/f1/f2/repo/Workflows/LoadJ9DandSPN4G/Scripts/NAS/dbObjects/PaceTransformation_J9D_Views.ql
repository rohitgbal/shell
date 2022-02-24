
--J9D LM Indirect view for USA data 
create view IF NOT EXISTS NCVDVCW_PACE_CMD_NAME_RSPNS_SEC_NA_USA_LMI_VW as 
select
*	
FROM NCVDCUM_PACE_CMD_NAME_RSPNS_SEC_HTE
WHERE cvdcum_partition_cntry_x ='USA';