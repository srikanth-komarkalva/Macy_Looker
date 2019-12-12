connection: "bq"

include: "*.view.lkml"                       # include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#
explore: bda_data {
  label: "BDA Reports"
  join: wip_summary {
    relationship: many_to_one
    sql_on: ${bda_data.rcpt_nbr} = ${wip_summary.rcpt_nbr} ;;
  }
}
