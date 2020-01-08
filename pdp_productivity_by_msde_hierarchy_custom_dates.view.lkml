view: pdp_productivity_by_msde_hierarchy_custom_dates {
  derived_table: {
    sql: select  PRDID as Product_ID,Proddesc as Product_Description,
      brnd_nm as Brand,prod_typ_desc as Product_Type,GREG_DT,Dept_Id,Dept_Desc, --display based on prompt
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      --SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) AS AUR,
      SUM(ITEM_QTY) AS units_Sold,

      --SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) AS Productivity,
      --(SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 AS View_to_Buy_Conv,
      --(SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100  AS Add_to_Bag_Conv,
      --(SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 AS Checkout_Conv,
      --(((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 AS MMU,

      --sum(LST_COST_AMT)/SUM(ITEM_QTY) as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      --(SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 AS Sell_Through_Rate,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      --(ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 AS Return_Rate,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews,
      SUM(SHOP_SESSN_CNT) as Shopping_Session,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,
      sum(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) AS BUY_SESSN_CNT

      from
      (
      select  PRD.web_prod_id AS PRDID,Prod_desc as Proddesc,brnd_nm,prod_typ_desc,--prc_typ_id,
      rpt_date.GREG_DT ,prd.mdse_dept_nbr as Dept_Id,prd.mdse_dept_desc as Dept_Desc,
      sum(VIEW_SESSN_PROD_CNT) AS VIEW_SESSN_PROD_CNT,
      SUM(TOT_SLS_AMT) AS TOT_SLS_AMT,
      SUM(ITEM_QTY) AS ITEM_QTY,
      SUM(VIEW_SESSN_CNT) AS VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) AS BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      sum(prod_age_nbr) as prod_age_nbr,
      0 as rtng_nbr,
      0 AS RVWS_CNT,
      0 AS STD_RTRN_QTY,
      0 AS STD_SLS_QTY,
      0 FOUR_WK_SLS_QTY,
      0 AS AVAIL_TO_SELL_QTY,
      0 AS oo_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      --INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.prc_typ_lkp_t_v` pricetype ON summary.MIN_PRC_TYP_ID=pricetype.PRC_TYP_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      --and rpt_date.GREG_DT BETWEEN '2019-10-04' AND '2019-10-10' ---- mandatory report filter Period A
      AND {% condition greg_dt %} >= filter_start_date {% endcondition %}
      AND {% condition greg_dt %} <= filter_end_date {% endcondition %}
      --and prd.mdse_dept_nbr=105 -- can be either dept no or mdse hierarchy
      --and prd.brnd_nm='Lee' -- optional report prompt
      group by  PRD.web_prod_id ,Prod_desc,brnd_nm,prod_typ_desc,--prc_typ_id,
      rpt_date.GREG_DT,prd.mdse_dept_nbr,prd.mdse_dept_desc
      union all

      select  PRD.web_prod_id AS PRDID,Prod_desc,brnd_nm,prod_typ_desc,--prc_typ_id,
      rpt_date.GREG_DT ,prd.mdse_dept_nbr as Dept_Id,prd.mdse_dept_desc as Dept_Desc,
      0 AS VIEW_SESSN_PROD_CNT,
      0 AS TOT_SLS_AMT,
      0 AS ITEM_QTY,
      0 AS VIEW_SESSN_CNT,
      0 AS BUY_SESSN_CNT,
      0 As SHOP_SESSN_CNT,
      0 as LST_COST_AMT,
      0 as prod_age_nbr,
      Sum(rtng_nbr) rtng_nbr,
      SUM(RVWS_CNT) AS RVWS_CNT,
      SUM(STD_RTRN_QTY) AS STD_RTRN_QTY,
      SUM(STD_SLS_QTY) as STD_SLS_QTY,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,
      SUM(AVAIL_TO_SELL_QTY) AS AVAIL_TO_SELL_QTY,
      SUM(oo_qty) AS On_OrderA

      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      --INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.prc_typ_lkp_t_v` pricetype ON summary.MIN_PRC_TYP_ID=pricetype.PRC_TYP_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      and {% condition greg_dt %} >= filter_end_date {% endcondition %} ---- mandatory report filter Period A
      --and prd.mdse_dept_nbr=105 -- can be either dept no or mdse hierarchy
      --and prd.brnd_nm='Lee' -- optional report prompt

      group by  PRD.web_prod_id ,Prod_desc
      ,brnd_nm,prod_typ_desc,--prc_typ_id,
      rpt_date.GREG_DT,prd.mdse_dept_nbr,prd.mdse_dept_desc

      )
      PeriodA

      group by PRDID,Proddesc,Brand,Product_Type,GREG_DT,Dept_Id,Dept_Desc
      --,brnd_nm,prod_typ_desc,prc_typ_id-- display based on the prompt
      order by confirmed_sales desc
 ;;
  }

  filter: date_filter {
    description: "Use this date filter in combination with the timeframes dimension for dynamic date filtering"
    type: date_time
    sql: {% condition date_filter %} cast(${TABLE}.GREG_DT as timestamp) {% endcondition %} ;;
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

  dimension: product_id {
    type: number
    sql: ${TABLE}.Product_ID ;;
  }

  dimension: product_description {
    type: string
    sql: ${TABLE}.Product_Description ;;
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.Brand ;;
  }

  dimension: product_type {
    type: string
    sql: ${TABLE}.Product_Type ;;
  }

  dimension: greg_dt {
    type: date
    sql: ${TABLE}.GREG_DT ;;
  }

  dimension: dept_id {
    type: number
    sql: ${TABLE}.Dept_id ;;
  }

  dimension: dept_desc {
    type: string
    sql: ${TABLE}.Dept_Desc ;;
  }

  dimension: Department {
    type: string
    sql: ${dept_id}||'-'||${dept_desc} ;;
  }

  measure: confirmed_sales {
    type: sum
    sql: ${TABLE}.Confirmed_Sales ;;
  }

  measure: aura {
    label: "AUR"
    type: number
    value_format: "$0.00"
    sql: NULLIF(${confirmed_sales},0)/NULLIF(${units_sold},0) ;;
  }

  measure: units_sold {
    type: sum
    sql: ${TABLE}.units_Sold ;;
  }

  measure: productivity {
    type: number
    sql: NULLIF(${confirmed_sales},0)/NULLIF(${view_sessn_cnt},0) ;;
  }

  measure: view_to_buy_conv_a {
    label: "View to Buy Conv"
    type: number
    value_format: "0.0\%"
    sql: NULLIF(${buy_sessn_cnt},0)/NULLIF(${view_sessn_cnt},0) ;;
  }

  measure: add_to_bag_conv {
    label: "Add to Bag Conv"
    type: number
    value_format: "0.0\%"
    sql: NULLIF(${shopping_session},0)/NULLIF(${view_sessn_cnt},0) ;;
  }

  measure: checkout_conv {
    label: "Checkout Conv"
    type: number
    value_format: "0.0\%"
    sql: NULLIF(${buy_sessn_cnt},0)/NULLIF(${shopping_session},0) ;;
  }

  measure: mmua {
    label: "MMU"
    type: number
    value_format: "0.0\%"
    sql: (((NULLIF(${confirmed_sales},0)/NULLIF(${units_sold},0)) - (NULLIF(${lst_cost_amt},0)/NULLIF(${units_sold},0)))/(NULLIF(${confirmed_sales},0)/NULLIF(${units_sold},0)))*100  ;;
  }

  measure: item_cost {
    label: "Item Cost"
    type: number
    value_format: "$0.00"
    sql: NULLIF(${lst_cost_amt},0)/NULLIF(${units_sold},0) ;;
  }

  measure: avail_to_sell {
    type: sum
    sql: ${TABLE}.Avail_to_Sell ;;
  }

  measure: on_order {
    type: sum
    sql: ${TABLE}.On_Order ;;
  }

  dimension: age {
    type: number
    sql: ${TABLE}.age ;;
  }

  measure: sell_through_rate_a {
    label: "Sell Through Rate"
    type: number
    value_format: "0.00\%"
    #sql:  ${four_wk_sls_qty}/(NULLIF(${four_wk_sls_qty},0) + ${avail_to_sell})*100 ;;
    sql:  ${four_wk_sls_qty}/((NULLIF(${four_wk_sls_qty},0) + NULLIF(${avail_to_sell},0))*100);;
  }

  measure: tot_unit_sold_std_qty {
    type: sum
    sql: ${TABLE}.Tot_Unit_Sold_Std_Qty ;;
  }

  measure: std_rtrn_unit_qty {
    type: sum
    sql: ${TABLE}.Std_Rtrn_Unit_Qty ;;
  }

  measure: return_rate_a {
    label: "Return Rate"
    type: number
    value_format: "0.00\%"
    sql: (NULLIF(${std_rtrn_unit_qty},0)/NULLIF(${tot_unit_sold_std_qty},0))*100 ;;
  }

  measure: product_rating {
    type: sum
    sql: ${TABLE}.Product_Rating ;;
  }

  measure: number_of_reviews {
    type: sum
    sql: ${TABLE}.Number_of_Reviews ;;
  }

  measure: shopping_session {
    type: sum
    sql: ${TABLE}.Shopping_Session ;;
  }

  measure: lst_cost_amt {
    type: sum
    sql: ${TABLE}.LST_COST_AMT ;;
  }

  measure: four_wk_sls_qty {
    type: sum
    sql: ${TABLE}.FOUR_WK_SLS_QTY ;;
  }

  measure: view_sessn_cnt {
    type: sum
    sql: ${TABLE}.VIEW_SESSN_CNT ;;
  }

  measure: buy_sessn_cnt {
    type: sum
    sql: ${TABLE}.BUY_SESSN_CNT ;;
  }

  set: detail {
    fields: [
      product_id,
      product_description,
      greg_dt,
      confirmed_sales,
      units_sold,
      avail_to_sell,
      on_order,
      sell_through_rate_a,
      age,
      tot_unit_sold_std_qty,
      std_rtrn_unit_qty,
      product_rating,
      number_of_reviews,
      shopping_session,
      lst_cost_amt,
      four_wk_sls_qty,
      view_sessn_cnt,
      buy_sessn_cnt
    ]
  }
}
