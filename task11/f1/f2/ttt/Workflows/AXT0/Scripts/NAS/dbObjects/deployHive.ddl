use ${hivevar:db_name};

--source LoadAXTSummary_DDL.ql;

--source createViews.ql

--source createViews_AXT_Summ.ql

source F2727617_add_colns.ql