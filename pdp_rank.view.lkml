view: pdp_rank {
  derived_table: {
    explore_source: pdp_draft_version {
      column:  prdid {}
      column: confirmed_sales {}
  #    column: rank_measure_dynamic_rank {}
      derived_column: rank {
        sql: row_number() over (order by rank_measure_dynamic_rank desc);;
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

              {% parameter pdp_rank.top_x %} +1 ;;
  }

#   parameter: rank_measure_selector {
#     label: "Rank Measure Selector"
#     type: unquoted
#
#     allowed_value: {
#       label: "Confirmed Sales"
#       value: "Confirmed_Sales"
#     }
#     allowed_value: {
#       label: "Aura"
#       value: "aura"
#     }
#     allowed_value: {
#       label: "Units Sold"
#       value: "units_Sold"
#     }
#   }
#
#   dimension: rank_measure_selector_dim {
#     label: "Measure Rank Dimension"
#     description: "To be used with the Rank Measure selector parameter"
#     label_from_parameter: rank_measure_selector
#     sql: ${TABLE}.{% parameter rank_measure_selector %};;
#   }
}
