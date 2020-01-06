view: price_type {
  derived_table: {
    sql:
      select  gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc ,
      sum(LiveProductA) LiveProductA,sum(LiveProductB) LiveProductB,(sum(LiveProductA)-sum(LiveProductB))/sum(LiveProductb) as PerVar,
      sum(ConfirmedSalesA) ConfirmedSalesA,sum(ConfirmedSalesB) ConfirmedSalesB,(sum(ConfirmedSalesA)-sum(ConfirmedSalesB))/sum(ConfirmedSalesB) as PerVar1,
      sum(units_soldA) units_soldA,sum(units_soldB) units_soldB,(sum(units_soldA)-sum(units_soldB))/sum(units_soldB)
      from
      (
      select gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc,
      case when key=1 then max(live_prod_ind) end   As LiveProductA,
      case when key=2 then max(live_prod_ind) end   As LiveProductB,
      case when key=1 then SUM(TOT_SLS_AMT) end ConfirmedSalesA,
      case when key=2 then SUM(TOT_SLS_AMT) end ConfirmedSalesB,
      case when key=1 then SUM(ITEM_QTY)  end AS units_soldA,
      case when key=2 then SUM(ITEM_QTY)  end AS units_soldB,

      --SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) as Aur,

      --(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) ) - (sum(lst_cost_amt)/SUM(ITEM_QTY))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) as MMU,
      --sum(lst_cost_amt)/SUM(ITEM_QTY) as itemcost,
      case when key=1 then sum(avail_to_sell_qty) end as AvailTosell,
      case when key=1 then Sum(oo_qty) end as OnOrder,
      --Sum(four_wk_sls_qty)/(Sum(four_wk_sls_qty)+sum(avail_to_sell_qty)) as SellThrough,
      case when key=1 then Sum(std_sls_qty) end as TotUnitSold,
      case when key=1 then sum(std_rtrn_qty) end as StdRtRnUnitQty
      --,abs(sum(std_rtrn_qty))/Sum(std_sls_qty) as ReturnRate
      from
      (
      select  1 as Key,prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
      SUM(TOT_SLS_AMT) TOT_SLS_AMT,
      sum(live_prod_ind) as live_prod_ind,
      SUM(ITEM_QTY) AS ITEM_QTY,
      0 as avail_to_sell_qty,
      0 as oo_qty,
      0 as std_sls_qty,
      0  as std_rtrn_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_prod_invntry_sls_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      and rpt_date.GREG_DT BETWEEN '2019-06-01' AND '2019-06-30' ---- mandatory report filter Period A
      --and prd.mdse_dept_nbr=280  and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT

      union all

      select 1 As Key, prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
      0 TOT_SLS_AMT,
      0 live_prod_ind,
      0 AS ITEM_QTY,
      sum(avail_to_sell_qty) as avail_to_sell_qty,
      Sum(oo_qty) as oo_qty,
      Sum(std_sls_qty) as std_sls_qty,
      sum(std_rtrn_qty) as std_rtrn_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_prod_invntry_sls_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      and rpt_date.GREG_DT = '2019-06-30' ---- mandatory report filter Period A
      --and prd.mdse_dept_nbr=280 and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT


      union all

      select  2 as Key,prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
      SUM(TOT_SLS_AMT) TOT_SLS_AMT,
      sum(live_prod_ind) as live_prod_ind,
      SUM(ITEM_QTY) AS ITEM_QTY,
      0 as avail_to_sell_qty,
      0 as oo_qty,
      0 as std_sls_qty,
      0  as std_rtrn_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_prod_invntry_sls_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      and rpt_date.GREG_DT BETWEEN '2019-09-01' AND '2019-09-30'---- mandatory report filter Period A
      --and prd.mdse_dept_nbr=280 and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT
      union all
      select 2 As Key, prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
      0 TOT_SLS_AMT,
      0 live_prod_ind,
      0 AS ITEM_QTY,
      sum(avail_to_sell_qty) as avail_to_sell_qty,
      Sum(oo_qty) as oo_qty,
      Sum(std_sls_qty) as std_sls_qty,
      sum(std_rtrn_qty) as std_rtrn_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_prod_invntry_sls_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      and rpt_date.GREG_DT = '2019-09-30' ---- mandatory report filter Period A
      --and prd.mdse_dept_nbr=280 and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT
      )PeriodA
      group by key,gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc
      )pricetype
      group by gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: gmm_id {
    type: string
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
    type: string
    sql: ${TABLE}.prc_typ_desc ;;
  }

  dimension: live_product_a {
    type: number
    sql: ${TABLE}.LiveProductA ;;
  }

  dimension: live_product_b {
    type: number
    sql: ${TABLE}.LiveProductB ;;
  }

  measure: sum_live_prod_a {
    type: sum
    hidden: yes
    sql: ${live_product_a} ;;
  }

  measure: sum_live_prod_b {
    type: sum
    hidden: yes
    sql: ${live_product_b} ;;
  }

  measure: diff_prod {
    type: number
    hidden: yes
    sql: ${sum_live_prod_a} - ${sum_live_prod_b} ;;
  }

  measure: var_prod {
    label: "(% VAR) Live Product"
    type: number
    sql: 100.0 * ${diff_prod} / NULLIF(${sum_live_prod_a}, 0);;
    value_format: "(0.00\%)"
  }

  dimension: per_var {
    type: number
    sql: ${TABLE}.PerVar ;;
  }

  dimension: confirmed_sales_a {
    type: number
    sql: ${TABLE}.ConfirmedSalesA ;;
  }

  dimension: confirmed_sales_b {
    type: number
    sql: ${TABLE}.ConfirmedSalesB ;;
  }

  measure: sum_confirmed_a {
    hidden: yes
    type: sum
    sql: ${confirmed_sales_a} ;;
  }

  measure: sum_confirmed_b {
    hidden: yes
    type: sum
    sql: ${confirmed_sales_b} ;;
  }

  measure: diff_confirmed {
    type: number
    hidden: yes
    sql: ${sum_confirmed_a} - ${sum_confirmed_b} ;;
  }

  measure: var_confirm {
    label: "(% VAR) Confirmed Sales"
    type: number
    sql: 100.0 * ${diff_confirmed} / NULLIF(${sum_confirmed_a}, 0);;
    value_format: "(0.00\%)"
  }

  dimension: per_var1 {
    type: number
    sql: ${TABLE}.PerVar1 ;;
  }

  dimension: units_sold_a {
    type: number
    sql: ${TABLE}.units_soldA ;;
  }

  dimension: units_sold_b {
    type: number
    sql: ${TABLE}.units_soldB ;;
  }

  measure: sum_units_sold_a {
    hidden: yes
    type: sum
    sql: ${units_sold_a} ;;
  }

  measure: sum_units_sold_b {
    hidden: yes
    type: sum
    sql: ${units_sold_b} ;;
  }

  measure: diff_units_sold {
    type: number
    hidden: yes
    sql: ${sum_units_sold_a} - ${sum_units_sold_b} ;;
  }

  measure: var_units_sold {
    label: "(% VAR)  Units Sold"
    type: number
    sql: 100.0 * ${diff_units_sold} / NULLIF(${sum_units_sold_a}, 0);;
    value_format: "(0.00\%)"
  }

  dimension: f0_ {
    type: number
    sql: ${TABLE}.f0_ ;;
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
      live_product_a,
      live_product_b,
      per_var,
      confirmed_sales_a,
      confirmed_sales_b,
      per_var1,
      units_sold_a,
      units_sold_b,
      f0_
    ]
  }
}
