create view if not exists nscvvepa_tcx_settings_na_usa_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='USA';
create view if not exists nscvvepa_tcx_settings_eu_fra_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='FRA';
create view if not exists nscvvepa_tcx_settings_eu_deu_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='DEU';
create view if not exists nscvvepa_tcx_settings_eu_ita_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='ITA';
create view if not exists nscvvepa_tcx_settings_eu_esp_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='ESP';
create view if not exists nscvvepa_tcx_settings_eu_gbr_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='GBR';
create view if not exists nscvvepa_tcx_settings_na_can_li_vw as select * from nscvpep_tcx_setg_summ_pii_hte where scvpep_partition_country_x='CAN';
