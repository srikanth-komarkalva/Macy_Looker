include: "Cognos_Reports.model.lkml"

view: digital_executive_summary {
  derived_table: {
    sql: select  GMMDESCA,
      max(VIEW_SESSN_PROD_CNT) as Product_Count,sum(TOT_SLS_AMT) as Confirmed_Sales,
      SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) AS AURA,
      SUM(ITEM_QTY) AS units_SoldA,
      SUM(VIEW_SESSN_CNT) AS Viewing_SessionA,
      SUM(BUY_SESSN_CNT) AS Buying_SessionA,
      SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) AS ProductivityA,
      (SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 AS View_to_Buy_ConvA,
      (SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100  AS Add_to_Bag_ConvA,
      (SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 AS Checkout_ConvA,
      (((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 AS MMUA,
      sum(LST_COST_AMT)/SUM(ITEM_QTY) as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_SellA,
      sum(oo_qty) AS On_OrderA,
      (SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 AS Sell_Through_RateA,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_QtyA,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_QtyA,
      (ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 AS Return_RateA,
      SUM(RVWS_CNT) AS Number_of_ReviewsAss,
      SUM(SHOP_SESSN_CNT) as Shopping_Session
      from
      (


      select  PRD.GMM_DESC AS GMMDESCA,
              rpt_date.GREG_DT ,
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
      and rpt_date.GREG_DT BETWEEN '2019-12-01' AND '2019-12-31' ---- mandatory report filter Period A
      --and PRD.GMM_DESC = 'CENTER CORE'
      group by  PRD.GMM_DESC ,rpt_date.GREG_DT

      union all

      select  PRD.GMM_DESC AS GMMDESCA,rpt_date.GREG_DT ,
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
      and rpt_date.GREG_DT >= '2019-12-01' ---- mandatory report filter Period A
      --and PRD.GMM_DESC = 'CENTER CORE'
      group by  PRD.GMM_DESC ,rpt_date.GREG_DT
      )

      PeriodA
      group by GMMDESCA
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: gmmdesca {
    label: "GMM"
    type: string
    sql: ${TABLE}.GMMDESCA ;;
  }

  dimension: product_count {
    label: "Product Count"
    type: number
    sql: ${TABLE}.Product_Count ;;
  }

  dimension: confirmed_sales {
    label: "Confirmed Sales"
    type: number
    sql: ${TABLE}.Confirmed_Sales ;;
  }

  dimension: aura {
    label: "AUR"
    type: number
    sql: ${TABLE}.AURA ;;
  }

  dimension: units_sold_a {
    label: "Units Sold"
    type: number
    sql: ${TABLE}.units_SoldA ;;
  }

  dimension: viewing_session_a {
    label: "Viewing Sessions"
    type: number
    sql: ${TABLE}.Viewing_SessionA ;;
  }

  dimension: buying_session_a {
    label: "Buying Sessions"
    type: number
    sql: ${TABLE}.Buying_SessionA ;;
  }

  dimension: productivity_a {
    label: "Productivity"
    type: number
    sql: ${TABLE}.ProductivityA ;;
  }

  dimension: view_to_buy_conv_a {
    label: "View to Buy Conv"
    type: number
    sql: ${TABLE}.View_to_Buy_ConvA ;;
  }

  dimension: add_to_bag_conv_a {
    label: "Add to Bag Conv"
    type: number
    sql: ${TABLE}.Add_to_Bag_ConvA ;;
  }

  dimension: checkout_conv_a {
    label: "Checkout Conv"
    type: number
    sql: ${TABLE}.Checkout_ConvA ;;
  }

  dimension: mmua {
    label: "MMU"
    type: number
    sql: ${TABLE}.MMUA ;;
  }

  dimension: item_cost {
    label: "Item Cost"
    type: number
    sql: ${TABLE}.Item_cost ;;
  }

  dimension: avail_to_sell_a {
    label: "Avail to Sell"
    type: number
    sql: ${TABLE}.Avail_to_SellA ;;
  }

  dimension: on_order_a {
    label: "On Order"
    type: number
    sql: ${TABLE}.On_OrderA ;;
  }

  dimension: sell_through_rate_a {
    label: "Sell Through Rate"
    type: number
    sql: ${TABLE}.Sell_Through_RateA ;;
  }

  dimension: tot_unit_sold_std_qty_a {
    label: "Tot Unit Sold Std Qty"
    type: number
    sql: ${TABLE}.Tot_Unit_Sold_Std_QtyA ;;
  }

  dimension: std_rtrn_unit_qty_a {
    label: "Std Rtrn Unit Qty"
    type: number
    sql: ${TABLE}.Std_Rtrn_Unit_QtyA ;;
  }

  dimension: return_rate_a {
    label: "Return Rate"
    type: number
    sql: ${TABLE}.Return_RateA ;;
  }

  dimension: number_of_reviews_ass {
    label: "Number of Reviews"
    type: number
    sql: ${TABLE}.Number_of_ReviewsAss ;;
  }

  dimension: shopping_session {
    type: number
    sql: ${TABLE}.Shopping_Session ;;
  }

  set: detail {
    fields: [
      gmmdesca,
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
