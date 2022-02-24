use ${hivevar:db_name};

source LoadH2TConsentSummary_DDL.ql;
source dropViews.ql;
source createViews.ql;

source LoadH2TConsentSummary_PJ1_DDL.ql;
$source createViews_PJ1.ql;