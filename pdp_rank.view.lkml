view: pdp_rank {
  derived_table: {
    explore_source: pdp_draft_version {
      column:  prdid {}
      column: confirmed_sales {}
      column: units_sold {}
      column: rank_measure_dynamic_rank {}
      derived_column: rank {
        sql: row_number() over (order by rank_measure_dynamic_rank desc);;
      }
      bind_all_filters: yes
    }
  }

  parameter: top_x {
    type: unquoted
    default_value: "5"
  }

  dimension: confirmed_sales {
    type: number
  }

  dimension: units_sold {
    type: number
  }

  dimension: rank {}
  dimension: prdid {}

  dimension: is_top_prdid {
    type: yesno
    sql: ${rank} <= {% parameter pdp_rank.top_x %} +1 ;;
  }
}
