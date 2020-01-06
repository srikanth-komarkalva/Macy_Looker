connection: "bq"

include: "digital_executive_summary.view.lkml"
include: "price_type.view.lkml"
include: "digital_executive_summary_testing.view.lkml"
include: "digital_executive_summary_product_b.view.lkml"


datagroup: macys_datagroup_cognos {
  ###Can be set to match your etl process
  sql_trigger: SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),hour) ;;
  max_cache_age: "1 hour"
}

persist_with: macys_datagroup_cognos

explore: digital_executive_summary {}
explore: price_type {}
explore: digital_executive_summary_testing{}
explore: digital_executive_summary_product_b {}
