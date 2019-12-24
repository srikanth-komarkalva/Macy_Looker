view: refresh_time {
  derived_table: {
    datagroup_trigger: macys_datagroup
    partition_keys: ["CURR_DATETIME_EST"]
    cluster_keys: ["CURR_DATE_EST","CURR_TIME_EST"]
    sql: SELECT TIMESTAMP(CURRENT_DATETIME("America/New_York")) AS CURR_DATETIME_EST
      ,DATE(CURRENT_DATETIME("America/New_York")) AS CURR_DATE_EST
      ,FORMAT_TIME("%T %p",CURRENT_TIME("America/New_York")) AS CURR_TIME_EST
      ,CASE
          WHEN FORMAT_TIME("%T %p",CURRENT_TIME("America/New_York")) >= FORMAT_TIME("%T %p", "00:00:00")
           AND FORMAT_TIME("%T %p",CURRENT_TIME("America/New_York")) < FORMAT_TIME("%T %p","11:30:00")
             THEN FORMAT_TIME("%T %p",CURRENT_TIME("America/New_York"))
          ELSE FORMAT_TIME("%T %p","11:30:00")
       END AS MIDDAY_DISP_TIME
 ;;
  }

  measure: count {
    type: count
#     drill_fields: [detail*]
  }

  dimension_group: curr_datetime_est {
    type: time
    sql: ${TABLE}.CURR_DATETIME_EST ;;
  }

  dimension: curr_date_est {
    type: date
    sql: ${TABLE}.CURR_DATE_EST ;;
  }

  dimension: Last_Refresh_Time {
    type: string
    sql:
    concat(
    CAST(${curr_date_est} AS STRING),
    " " ,
    CAST(${curr_time_est} AS STRING)
    )
    ;;
  }

  dimension: curr_time_est {
    type: string
    sql: ${TABLE}.CURR_TIME_EST ;;
  }

  dimension: midday_disp_time {
    type: string
    sql: ${TABLE}.MIDDAY_DISP_TIME ;;
  }

  set: detail {
    fields: [curr_datetime_est_time, curr_date_est, curr_time_est, midday_disp_time]
  }
}
