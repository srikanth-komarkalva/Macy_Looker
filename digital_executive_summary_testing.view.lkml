view: digital_executive_summary_testing {
  derived_table: {
    sql: select  GMMDESCA, GREG_DT,
      max(VIEW_SESSN_PROD_CNT) as Product_Count,sum(TOT_SLS_AMT) as Confirmed_Sales,
   -- SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) AS AURA,
      SUM(ITEM_QTY) AS units_SoldA,
      SUM(VIEW_SESSN_CNT) AS Viewing_SessionA,
      SUM(BUY_SESSN_CNT) AS Buying_SessionA,
      SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) AS ProductivityA,
      (SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 AS View_to_Buy_ConvA,
      (SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100  AS Add_to_Bag_ConvA,
      (SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 AS Checkout_ConvA,
     -- (((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 AS MMUA,
      --sum(LST_COST_AMT)/SUM(ITEM_QTY) as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_SellA,
      sum(oo_qty) AS On_OrderA,
     -- (SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 AS Sell_Through_RateA,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_QtyA,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_QtyA,
     --(ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 AS Return_RateA,
      SUM(RVWS_CNT) AS Number_of_ReviewsAss,
      SUM(SHOP_SESSN_CNT) as Shopping_Session,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY
      from
      (


      select  PRD.GMM_DESC AS GMMDESCA,
              rpt_date.GREG_DT AS GREG_DT,
        sum(VIEW_SESSN_PROD_CNT) AS VIEW_SESSN_PROD_CNT,
        SUM(TOT_SLS_AMT) AS TOT_SLS_AMT,
        SUM(ITEM_QTY) AS ITEM_QTY,
        SUM(VIEW_SESSN_CNT) AS VIEW_SESSN_CNT,
        SUM(BUY_SESSN_CNT) AS BUY_SESSN_CNT,
        SUM(SHOP_SESSN_CNT) SHOP_SESSN_CNT,
        SUM(LST_COST_AMT) as LST_COST_AMT,
      0 AS RVWS_CNT,
      0 AS STD_RTRN_QTY,
      0 AS STD_SLS_QTY,
      0 FOUR_WK_SLS_QTY,
      0 AS AVAIL_TO_SELL_QTY,
      0 AS oo_qty

      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      WHERE
      Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
     -- and rpt_date.GREG_DT BETWEEN '2018-01-01' AND '2019-12-31' ---- mandatory report filter Period A
       AND {% condition greg_dt %} >= filter_start_date {% endcondition %}
       AND {% condition greg_dt %} <= filter_end_date {% endcondition %}
      --and PRD.GMM_DESC = 'CENTER CORE'
      group by  PRD.GMM_DESC ,rpt_date.GREG_DT

      union all

      select  PRD.GMM_DESC AS GMMDESCA,
              rpt_date.GREG_DT as GREG_DT,
      0 AS VIEW_SESSN_PROD_CNT,
      0 AS TOT_SLS_AMT,
      0 AS ITEM_QTY,
      0 AS VIEW_SESSN_CNT,
      0 AS BUY_SESSN_CNT,
      0 As SHOP_SESSN_CNT,
      0 as LST_COST_AMT,
      SUM(RVWS_CNT) AS RVWS_CNT,
      SUM(STD_RTRN_QTY) AS STD_RTRN_QTY,
      SUM(STD_SLS_QTY) as STD_SLS_QTY,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,
      SUM(AVAIL_TO_SELL_QTY) AS AVAIL_TO_SELL_QTY,
      SUM(oo_qty) AS On_OrderA

      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      and {% condition greg_dt %} >= filter_end_date {% endcondition %} ---- mandatory report filter Period A
      --and PRD.GMM_DESC = 'CENTER CORE'
      group by  PRD.GMM_DESC ,rpt_date.GREG_DT
      )

      PeriodA
      group by GMMDESCA
      , GREG_DT
 ;;
  }

  filter: date_filter {
    description: "Use this date filter in combination with the timeframes dimension for dynamic date filtering"
    type: date
    sql: {% condition date_filter %} ${greg_dt} {% endcondition %} ;;
  }

  dimension: filter_start_date {
    type: date
    sql: CAST(
    CASE WHEN {% date_start date_filter %} IS NULL THEN '2018-01-01' ELSE NULLIF({% date_start date_filter %}, 0) END
     AS timestamp) ;;
  }

  dimension: filter_end_date {
    type: date
    sql: CAST(
    CASE WHEN {% date_end date_filter %} IS NULL THEN '2020-01-01' ELSE NULLIF({% date_end date_filter %}, 0) END
    as timestamp);;
 }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: gmmdesca {
    type: string
    sql: ${TABLE}.GMMDESCA ;;
  }

  dimension: greg_dt {
    type: date
    sql: cast(${TABLE}.GREG_DT as timestamp) ;;
  }

  measure: product_count {
    type: sum
    sql: ${TABLE}.Product_Count ;;
  }

  measure: lst_cost_amt {
    type: sum
    sql: ${TABLE}.LST_COST_AMT ;;
  }

  measure: four_wk_sls_qty {
    type: sum
    sql: ${TABLE}.FOUR_WK_SLS_QTY ;;
  }

  measure: aura {
    type: number
    sql: ${confirmed_sales}/${units_sold_a} ;;
  }

  measure: mmua {
    type: number
    sql: (((${confirmed_sales}/${units_sold_a}) - (${lst_cost_amt}/${units_sold_a}))/(${confirmed_sales}/${units_sold_a}))*100  ;;
  }

  measure: confirmed_sales {
    type: sum
    sql: ${TABLE}.Confirmed_Sales ;;
  }

  measure: units_sold_a {
    type: sum
    sql: ${TABLE}.units_SoldA ;;
  }

  measure: item_cost {
    type: number
    sql: ${lst_cost_amt}/${units_sold_a} ;;
  }

  measure: sell_through_rate_a {
    type: number
    sql:  (${four_wk_sls_qty}/(${four_wk_sls_qty} + ${avail_to_sell_a}))*100 ;;
  }

  measure: return_rate_a {
    type: number
    sql: (${std_rtrn_unit_qty_a}/${tot_unit_sold_std_qty_a})*100 ;;
  }

  measure: viewing_session_a {
    type: sum
    sql: ${TABLE}.Viewing_SessionA ;;
  }

  measure: buying_session_a {
    type: sum
    sql: ${TABLE}.Buying_SessionA ;;
  }

  measure: productivity_a {
    type: sum
    sql: ${TABLE}.ProductivityA ;;
  }

  measure: view_to_buy_conv_a {
    type: sum
    sql: ${TABLE}.View_to_Buy_ConvA ;;
  }

  measure: add_to_bag_conv_a {
    type: sum
    sql: ${TABLE}.Add_to_Bag_ConvA ;;
  }

  measure: checkout_conv_a {
    type: sum
    sql: ${TABLE}.Checkout_ConvA ;;
  }

  measure: avail_to_sell_a {
    type: sum
    sql: ${TABLE}.Avail_to_SellA ;;
  }

  measure: on_order_a {
    type: sum
    sql: ${TABLE}.On_OrderA ;;
  }

  measure: tot_unit_sold_std_qty_a {
    type: sum
    sql: ${TABLE}.Tot_Unit_Sold_Std_QtyA ;;
  }

  measure: std_rtrn_unit_qty_a {
    type: sum
    sql: ${TABLE}.Std_Rtrn_Unit_QtyA ;;
  }

  measure: number_of_reviews_ass {
    type: sum
    sql: ${TABLE}.Number_of_ReviewsAss ;;
  }

  measure: shopping_session {
    type: sum
    sql: ${TABLE}.Shopping_Session ;;
  }

  set: detail {
    fields: [
      gmmdesca,
      greg_dt,
      product_count,
      confirmed_sales,
      aura,
      units_sold_a,
      viewing_session_a,
      buying_session_a,
      productivity_a,
      view_to_buy_conv_a,
      add_to_bag_conv_a,
      checkout_conv_a,
      mmua,
      item_cost,
      avail_to_sell_a,
      on_order_a,
      sell_through_rate_a,
      tot_unit_sold_std_qty_a,
      std_rtrn_unit_qty_a,
      return_rate_a,
      number_of_reviews_ass,
      shopping_session
    ]
  }
}
