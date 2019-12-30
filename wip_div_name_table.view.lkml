include: "BDA_Reports.model.lkml"

view: wip_div_name_table {
  derived_table: {
    sql: select "12" as LOC_NBR, "Macys" as LOCN_NAME,"MCY" as LOCN_ABBR,"71" as ZL_DIVN_NBR
      UNION ALL
      select "13" as LOC_NBR, "Bloomindales" as LOCN_NAME,"BLM" as LOCN_ABBR,"72" as ZL_DIVN_NBR
      UNION ALL
      select "499" as LOC_NBR, "MACYS>COM-L.." as LOCN_NAME,"MDC" as LOCN_ABBR,"88" as ZL_DIVN_NBR
      UNION ALL
      select "1027" as LOC_NBR, "BLOOMINGA.." as LOCN_NAME,"BOS" as LOCN_ABBR,"79" as ZL_DIVN_NBR
      UNION ALL
      select "5842" as LOC_NBR, "BLOOMINGA.." as LOCN_NAME,"BDC" as LOCN_ABBR,"76" as ZL_DIVN_NBR
      UNION ALL
      select "6315" as LOC_NBR, "Macys Backsta.." as LOCN_NAME,"MBK" as LOCN_ABBR,"77" as ZL_DIVN_NBR
      UNION ALL
      select "6500" as LOC_NBR, "BLUEMERCURY" as LOCN_NAME,"BLU" as LOCN_ABBR,"78" as ZL_DIVN_NBR
      UNION ALL
      select "6945" as LOC_NBR, "MACYS CHINA" as LOCN_NAME,"MCL" as LOCN_ABBR,"85" as ZL_DIVN_NBR
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: loc_nbr {
    type: string
    sql: ${TABLE}.LOC_NBR ;;
  }

  dimension: locn_name {
    type: string
    sql: ${TABLE}.LOCN_NAME ;;
  }

  dimension: locn_abbr {
    type: string
    sql: ${TABLE}.LOCN_ABBR ;;
  }

  dimension: zl_divn_nbr {
    type: string
    sql: ${TABLE}.ZL_DIVN_NBR ;;
  }

  dimension: div_nbr_name {
    type: string
    sql: ${zl_divn_nbr}+'-'+${locn_name}  ;;
  }

  set: detail {
    fields: [loc_nbr, locn_name, locn_abbr, zl_divn_nbr]
  }
}
