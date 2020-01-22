connection: "bq"

include: "price_type.view.lkml"
include: "digital_executive_summary_testing.view.lkml"
include: "pdp_productivity_by_msde_hierarchy_custom_dates.view.lkml"
include: "pdp_summary_totals.view.lkml"
include: "price.view.lkml"
include: "pdp_draft_version.view.lkml"
include: "price_type_a.view.lkml"
include: "price_type_b.view.lkml"
include: "pdp_rank.view.lkml"



datagroup: macys_datagroup_cognos {
  ###Can be set to match your etl process
  sql_trigger: SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),hour) ;;
  max_cache_age: "1 hour"
}

persist_with: macys_datagroup_cognos


explore: price_type {}
explore: digital_executive_summary_testing {}

explore: pdp_productivity_by_msde_hierarchy_custom_dates {}
explore: pdp_summary_totals {}
explore: price {}

explore: pdp_draft_version {
  join: pdp_rank {
    type: inner
    relationship: many_to_one
    sql_on: ${pdp_draft_version.prdid}=${pdp_rank.prdid};;
  }
}



explore: price_type_a {}
explore: price_type_b {}

# explore: price_type_a {
# explore:  price_type_b {
#     relationship: many_to_many
#     type: full_outer
#     sql_on: ${price_type_a.prc_typ_id}=${price_type_b.prc_typ_id}
#             and ${price_type_a.mdse_dept_nbr}=${price_type_b.mdse_dept_nbr} ;;
#   }
# }
