view: pdp_rank {
  derived_table: {
    explore_source: pdp_draft_version {
      column:  prdid {}
      column: confirmed_sales {}
      derived_column: rank {
        sql: row_number() over (order by confirmed_sales desc);;
      }
      bind_all_filters: yes
    }
  }

  dimension: prdid {}

  parameter: top_x {
    type: unquoted
    default_value: "5"
  }

  dimension: confirmed_sales {
    type: number
  }

  dimension: rank {}

  dimension: is_top_prdid {
    type: yesno
    sql: ${rank} <=

              {% parameter pdp_rank.top_x %}

              ;;
  }
}
