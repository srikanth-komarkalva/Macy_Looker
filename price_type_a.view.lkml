view: price_type_a {
  derived_table: {
    sql: select gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc,GREG_DT,
      max(live_prod_ind)   As LiveProductA,
      SUM(TOT_SLS_AMT) as ConfirmedSalesA,
      SUM(ITEM_QTY)  AS units_soldA,
      sum(lst_cost_amt) as lst_cost_amt_a,
      --SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) as Aur,
      --(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) ) - (sum(lst_cost_amt)/SUM(ITEM_QTY))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) as MMU,
      --sum(lst_cost_amt)/SUM(ITEM_QTY) as itemcost,
      sum(avail_to_sell_qty) as AvailTosell_A,
      Sum(oo_qty) as OnOrder_A,
      sum(four_wk_sls_qty) as four_wk_sls_qty_a,
      --Sum(four_wk_sls_qty)/(Sum(four_wk_sls_qty)+sum(avail_to_sell_qty)) as SellThrough,
      Sum(std_sls_qty) as TotUnitSold_A,
      sum(std_rtrn_qty) as StdRtRnUnitQty_A
      --,abs(sum(std_rtrn_qty))/Sum(std_sls_qty) as ReturnRate
      from(


select  1 as Key,prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
      SUM(TOT_SLS_AMT) TOT_SLS_AMT,
      sum(live_prod_ind) as live_prod_ind,
      SUM(ITEM_QTY) AS ITEM_QTY,
      sum(lst_cost_amt) as lst_cost_amt,
      0 as avail_to_sell_qty,
      0 as oo_qty,
      0 as std_sls_qty,
      0  as std_rtrn_qty,
      0 as four_wk_sls_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_prod_invntry_sls_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      --and rpt_date.GREG_DT BETWEEN '2019-06-01' AND '2019-06-30' ---- mandatory report filter Period A
      AND {% condition greg_dt %} >= filter_start_date {% endcondition %}
      AND {% condition greg_dt %} <= filter_end_date {% endcondition %}
      --and prd.mdse_dept_nbr=280  and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT

      union all

      select 1 As Key, prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
      0 TOT_SLS_AMT,
      0 live_prod_ind,
      0 AS ITEM_QTY,
      0 AS lst_cost_amt,
      sum(avail_to_sell_qty) as avail_to_sell_qty,
      Sum(oo_qty) as oo_qty,
      Sum(std_sls_qty) as std_sls_qty,
      sum(std_rtrn_qty) as std_rtrn_qty,
      sum(four_wk_sls_qty) as four_wk_sls_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_prod_invntry_sls_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      --and rpt_date.GREG_DT = '2019-06-30' ---- mandatory report filter Period A
      and {% condition greg_dt %} >= filter_end_date {% endcondition %}
      --and prd.mdse_dept_nbr=280 and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT
)
group by gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc,GREG_DT
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: gmm_id {
    type: number
    sql: ${TABLE}.gmm_id ;;
  }

  dimension: gmm_desc {
    type: string
    sql: ${TABLE}.gmm_desc ;;
  }

  dimension: mdse_divn_mgr_desc {
    type: string
    sql: ${TABLE}.mdse_divn_mgr_desc ;;
  }

  dimension: mdse_divn_mgr_id {
    type: number
    sql: ${TABLE}.mdse_divn_mgr_id ;;
  }

  dimension: mdse_dept_nbr {
    type: number
    sql: ${TABLE}.mdse_dept_nbr ;;
  }

  dimension: mdse_dept_desc {
    type: string
    sql: ${TABLE}.mdse_dept_desc ;;
  }

  dimension: prc_grp_cd {
    type: string
    sql: ${TABLE}.prc_grp_cd ;;
  }

  dimension: prc_typ_id {
    type: number
    sql: ${TABLE}.prc_typ_id ;;
  }

  dimension: prc_typ_desc {
    label: "Price Type Group"
    type: string
    sql: ${TABLE}.prc_typ_desc ;;
  }

  dimension: greg_dt {
    type: date
    sql: ${TABLE}.GREG_DT ;;
  }

  measure: live_product_a {
    label: "Live Product Period A"
    type: sum
    sql: ${TABLE}.LiveProductA ;;
  }

  measure: confirmed_sales_a {
    type: sum
    label: "Confirmed Sales Period A"
    value_format: "$0.00"
    sql: ${TABLE}.ConfirmedSalesA ;;
  }

  measure: units_sold_a {
    type: sum
    label: "Units Sold Period A"
    sql: ${TABLE}.units_soldA ;;
  }

  measure: lst_cost_amt_a {
    type: sum
    sql: ${TABLE}.lst_cost_amt_a ;;
  }

  measure: avail_tosell_a {
    label: "Avail to Sell Period A"
    type: sum
    sql: ${TABLE}.AvailTosell_A ;;
  }

  measure: on_order_a {
    type: sum
    label:"On Order Period A"
    sql: ${TABLE}.OnOrder_A ;;
  }

  measure: four_wk_sls_qty_a {
    type: sum
    sql: ${TABLE}.four_wk_sls_qty_a ;;
  }

  measure: tot_unit_sold_a {
    type: sum
    label: "Tot Unit Sold Std Qty Period A"
    sql: ${TABLE}.TotUnitSold_A ;;
  }

  measure: std_rt_rn_unit_qty_a {
    type: sum
    label: "Std Rtrn Unit Qty Period A"
    sql: ${TABLE}.StdRtRnUnitQty_A ;;
  }

  measure: aura {
    label: "AUR Period A"
    type: number
    value_format: "$0.00"
    sql: ${confirmed_sales_a}/NULLIF(${units_sold_a}, 0) ;;
  }

  measure: mmua {
    label: "MMU Period A"
    type: number
    value_format: "0.0\%"
    sql: (((NULLIF(${confirmed_sales_a},0)/NULLIF(${units_sold_a},0)) - (NULLIF(${lst_cost_amt_a},0)/NULLIF(${units_sold_a},0)))/(NULLIF(${confirmed_sales_a},0)/NULLIF(${units_sold_a},0)))*100  ;;
  }

  measure: item_costa {
    label: "Item Cost Period A"
    type: number
    value_format: "$0.00"
    sql: ${lst_cost_amt_a}/NULLIF(${units_sold_a}, 0) ;;
  }

  measure: sell_through_rate_a {
    label: "Sell Through Rate Period A"
    type: number
    value_format: "0.00\%"
    #sql:  (${four_wk_sls_qty_a}/NULLIF((${four_wk_sls_qty_a}, 0) + ${avail_tosell_a}))*100 ;;

    sql:  (${four_wk_sls_qty_a}/NULLIF((${four_wk_sls_qty_a} + ${avail_tosell_a}), 0))*100 ;;
  }

  measure: return_rate_a {
    label: "Return Rate Period A"
    type: number
    value_format: "0.00\%"
    sql: (${std_rt_rn_unit_qty_a}/NULLIF(${tot_unit_sold_a}, 0))*100 ;;
  }

  set: detail {
    fields: [
      gmm_id,
      gmm_desc,
      mdse_divn_mgr_desc,
      mdse_divn_mgr_id,
      mdse_dept_nbr,
      mdse_dept_desc,
      prc_grp_cd,
      prc_typ_id,
      prc_typ_desc,
      greg_dt,
      live_product_a,
      confirmed_sales_a,
      units_sold_a,
      lst_cost_amt_a,
      avail_tosell_a,
      on_order_a,
      four_wk_sls_qty_a,
      tot_unit_sold_a,
      std_rt_rn_unit_qty_a
    ]
  }
}
