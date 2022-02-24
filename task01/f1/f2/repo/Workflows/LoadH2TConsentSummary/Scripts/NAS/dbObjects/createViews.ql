create view IF NOT EXISTS NCVDVIK_H2T_SUMM_PII_NA_USA_LMI_VW 
as
select 
*
FROM NSCVP99_H2T_NA_USA_SUMM_PII_HTE;

create view IF NOT EXISTS NSCVV99A_H2T_SUMM_NA_USA_LI_VW 
as
select 
*
FROM NSCVP99_H2T_NA_USA_SUMM_PII_HTE;


create view IF NOT EXISTS NSCVV99A_H2T_SUMM_EU_FRA_LI_VW 
as
select 
*
FROM NSCVP99_H2T_EU_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'FRA';

create view IF NOT EXISTS NSCVV99A_H2T_SUMM_EU_GBR_LI_VW
as
select
*
FROM NSCVP99_H2T_EU_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'GBR';

create view IF NOT EXISTS NSCVV99A_H2T_SUMM_EU_DEU_LI_VW 
as
select 
*
FROM NSCVP99_H2T_EU_NON_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'DEU';


create view IF NOT EXISTS NSCVV99A_H2T_SUMM_EU_ITA_LI_VW 
as
select 
*
FROM NSCVP99_H2T_EU_NON_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'ITA';


create view IF NOT EXISTS NSCVV99A_H2T_SUMM_EU_ESP_LI_VW 
as
select 
*
FROM NSCVP99_H2T_EU_NON_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'ESP';



create view IF NOT EXISTS NSCVV99A_H2T_SUMM_NA_CAN_LI_VW 
as
select 
*
FROM NSCVP99_H2T_NA_NON_USA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'CAN';

-- VIEW Creation for OIC H2T EU, Feature Id F327601

CREATE VIEW IF NOT EXISTS NSCVV99A_H2T_SUMM_EU_REG_LI_VW AS 
SELECT
*
FROM NSCVP99_H2T_EU_NON_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x in
('AUT','BEL','BGR','HRV','CYP','CZE','DNK','EST','FIN','DEU','GRC','HUN','IRL','ITA','LVA','LTU','LUX','MLT','NLD','POL','PRT','ROU','SVK','SVN','ESP','SWE','AT','BE','BG','HR','CY','CZ','DK','EE','FI','FR','DE','GR','HU','IE','IT','LV','LT','LU','MT','NL','PL','PT','RO','SK','SI','ES','SE','ROM');
UNION
select
*
FROM NSCVP99_H2T_EU_GBR_FRA_SUMM_PII_HTE
WHERE scvp99_partition_cntry_x = 'FRA';