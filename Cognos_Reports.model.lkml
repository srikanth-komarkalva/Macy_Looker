connection: "bq"

include: "price_type.view.lkml"
include: "digital_executive_summary_testing.view.lkml"
include: "pdp_productivity_by_msde_hierarchy_custom_dates.view.lkml"
include: "pdp_summary_totals.view.lkml"



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



# explore: bda_data {
#   label: "BDA Reports"
#   join: wip_summary {
#     relationship: many_to_one
#     type: left_outer
#     sql_on: ${bda_data.rcpt_nbr} = ${wip_summary.rcpt_nbr} ;;
#   }
