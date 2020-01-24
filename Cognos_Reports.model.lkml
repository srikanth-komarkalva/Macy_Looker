connection: "bq"

include: "digital_executive_summary_testing.view.lkml"
include: "pdp_summary_totals.view.lkml"
include: "pdp_draft_version.view.lkml"
include: "price_type_a.view.lkml"
include: "price_type_b.view.lkml"
include: "pdp_rank.view.lkml"

datagroup: macys_datagroup_cognos {
  sql_trigger: SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),hour) ;;
  max_cache_age: "1 hour"
}

persist_with: macys_datagroup_cognos

explore: digital_executive_summary_testing {}

explore: pdp_summary_totals {}

explore: pdp_draft_version {
  join: pdp_rank {
    type: inner
    relationship: many_to_one
    sql_on: ${pdp_draft_version.prdid}=${pdp_rank.prdid};;
  }
}

explore: price_type_a {}
explore: price_type_b {}
