connection: "bq"

include: "price_type.view.lkml"
include: "digital_executive_summary_testing.view.lkml"
include: "pdp_productivity_by_msde_hierarchy_custom_dates.view.lkml"



datagroup: macys_datagroup_cognos {
  ###Can be set to match your etl process
  sql_trigger: SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),hour) ;;
  max_cache_age: "1 hour"
}

persist_with: macys_datagroup_cognos


explore: price_type {}
explore: pdp_productivity_by_msde_hierarchy_custom_dates {}
explore: digital_executive_summary_testing {}
