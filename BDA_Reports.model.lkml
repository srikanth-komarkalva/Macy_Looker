connection: "bq"

include: "*.view.lkml"                       # include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#

datagroup: macys_datagroup {
  ###Can be set to match your etl process
#   sql_trigger: SELECT max(completed_at) FROM public.etl_jobs ;;
  sql_trigger: SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),hour) ;;
  max_cache_age: "1 hour"
}

persist_with: macys_datagroup

explore: bda_data {
  label: "BDA Reports"
  join: wip_summary {
    relationship: many_to_one
    type: left_outer
    sql_on: ${bda_data.rcpt_nbr} = ${wip_summary.rcpt_nbr} ;;
  }
}

# Waves In Progress

explore: casestopick {
  label: "Waves in progress Reports"
  join: wavesinprogress_summary {
    relationship: many_to_one
    type: left_outer
    sql_on: ${casestopick.wave_number} = ${wavesinprogress_summary.wave_number} ;;
  }
}

#preSortDashboard

explore: containers {
  label: "preSortDasboard"
  join: presortdashboard {
    relationship: many_to_one
    type: left_outer
    sql_on: ${containers.wave_number} = ${presortdashboard.wave_number} ;;
  }
}
# Last Refresh time
explore: refresh_time {}
