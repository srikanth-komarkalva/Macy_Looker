connection: "bq"

include: "price_type.view.lkml"
include: "digital_executive_summary_testing.view.lkml"
include: "pdp_productivity_by_msde_hierarchy_custom_dates.view.lkml"
include: "pdp_summary_totals.view.lkml"
include: "price.view.lkml"
include: "pdp_draft_version.view.lkml"
include: "price_type_a.view.lkml"
include: "price_type_b.view.lkml"
include: "draft_query.view.lkml"


datagroup: macys_datagroup_cognos {
  ###Can be set to match your etl process
  sql_trigger: SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),hour) ;;
  max_cache_age: "1 hour"
}

persist_with: macys_datagroup_cognos


explore: price_type {}
explore: digital_executive_summary_testing {}

explore: pdp_productivity_by_msde_hierarchy_custom_dates {
  join:  pdp_summary_totals {
    relationship: one_to_one
    type: left_outer
    sql_on: ${pdp_productivity_by_msde_hierarchy_custom_dates.dept_id}=${pdp_summary_totals.mdse_dept_nbr} ;;
  }
  }

explore: price {}
explore: pdp_draft_version {}
explore: price_type_a {}
explore: price_type_b {}
explore: draft_query {}

# explore: price_type_a {
# explore:  price_type_b {
#     relationship: many_to_many
#     type: full_outer
#     sql_on: ${price_type_a.prc_typ_id}=${price_type_b.prc_typ_id}
#             and ${price_type_a.mdse_dept_nbr}=${price_type_b.mdse_dept_nbr} ;;
#   }
# }
