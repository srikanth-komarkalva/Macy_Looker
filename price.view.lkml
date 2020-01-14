view: price {
  derived_table: {
    sql: select  gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc ,GREG_DT,
      sum(LiveProductA) LiveProductA,sum(LiveProductB) LiveProductB,
      sum(ConfirmedSalesA) ConfirmedSalesA,sum(ConfirmedSalesB) ConfirmedSalesB,
      sum(units_soldA) units_soldA,sum(units_soldB) units_soldB,
      sum(lst_cost_amt_a) as lst_cost_amt_a,sum(lst_cost_amt_b) as lst_cost_amt_b,
      sum(AvailTosell_A) as AvailTosell_A,sum(AvailTosell_B) as AvailTosell_B,
      sum(OnOrder_A) as OnOrder_A,sum(OnOrder_B) as OnOrder_B,
      sum(four_wk_sls_qty_a) as four_wk_sls_qty_a,sum(four_wk_sls_qty_b) as four_wk_sls_qty_b,
      sum(TotUnitSold_A) as TotUnitSold_A,sum(TotUnitSold_B) as TotUnitSold_B,
      sum(StdRtRnUnitQty_A) as StdRtRnUnitQty_A,sum(StdRtRnUnitQty_B) as StdRtRnUnitQty_B

      from
      (
      select gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc,GREG_DT,
      case when key=1 then max(live_prod_ind) end   As LiveProductA,
      case when key=2 then max(live_prod_ind) end   As LiveProductB,
      case when key=1 then SUM(TOT_SLS_AMT) end ConfirmedSalesA,
      case when key=2 then SUM(TOT_SLS_AMT) end ConfirmedSalesB,
      case when key=1 then SUM(ITEM_QTY)  end AS units_soldA,
      case when key=2 then SUM(ITEM_QTY)  end AS units_soldB,
      case when key=1 then sum(lst_cost_amt) end as lst_cost_amt_a,
      case when key=2 then sum(lst_cost_amt) end as lst_cost_amt_b,

      --SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) as Aur,

      --(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) ) - (sum(lst_cost_amt)/SUM(ITEM_QTY))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) as MMU,
      --sum(lst_cost_amt)/SUM(ITEM_QTY) as itemcost,
      case when key=1 then sum(avail_to_sell_qty) end as AvailTosell_A,
      case when key=2 then sum(avail_to_sell_qty) end as AvailTosell_B,
      case when key=1 then Sum(oo_qty) end as OnOrder_A,
      case when key=2 then Sum(oo_qty) end as OnOrder_B,
      case when key=1 then sum(four_wk_sls_qty) end as four_wk_sls_qty_a,
      case when key=2 then sum(four_wk_sls_qty) end as four_wk_sls_qty_b,
      --Sum(four_wk_sls_qty)/(Sum(four_wk_sls_qty)+sum(avail_to_sell_qty)) as SellThrough,
      case when key=1 then Sum(std_sls_qty) end as TotUnitSold_A,
      case when key=2 then Sum(std_sls_qty) end as TotUnitSold_B,
      case when key=1 then sum(std_rtrn_qty) end as StdRtRnUnitQty_A,
      case when key=2 then sum(std_rtrn_qty) end as StdRtRnUnitQty_B
      --,abs(sum(std_rtrn_qty))/Sum(std_sls_qty) as ReturnRate
      from
      (
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


      union all

      select  2 as Key,prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
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
      --and rpt_date.GREG_DT BETWEEN '2019-09-01' AND '2019-09-30'---- mandatory report filter Period A
      AND {% condition greg_dt %} >= filter_start_date_1 {% endcondition %}
      AND {% condition greg_dt %} <= filter_end_date_1 {% endcondition %}
      --and prd.mdse_dept_nbr=280 and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT
      union all
      select 2 As Key, prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT ,
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
      --and rpt_date.GREG_DT = '2019-09-30' ---- mandatory report filter Period A
      and {% condition greg_dt %} >= filter_end_date_1 {% endcondition %}
      --and prd.mdse_dept_nbr=280 and summary.prc_grp_cd='MKD'
      group by  prd.gmm_id,prd.gmm_desc,prd.mdse_divn_mgr_desc,prd.mdse_divn_mgr_id,prd.mdse_dept_nbr,prd.mdse_dept_desc,summary.prc_grp_cd,summary.prc_typ_id,summary.prc_typ_desc,rpt_date.GREG_DT
      )PeriodA
      group by key,gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc,GREG_DT
      )pricetype
      group by gmm_id,gmm_desc,mdse_divn_mgr_desc,mdse_divn_mgr_id,mdse_dept_nbr,mdse_dept_desc,prc_grp_cd,prc_typ_id,prc_typ_desc,GREG_DT
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

  filter: date_filter_1 {
    description: "Use this date filter in combination with the timeframes dimension for dynamic date filtering"
    type: date_time
    sql: {% condition date_filter %} cast(${TABLE}.GREG_DT as timestamp) {% endcondition %} ;;
  }

  dimension: filter_start_date_1 {
    type: date
    sql: CAST(
          CASE WHEN {% date_start date_filter_1 %} IS NULL THEN '2018-01-01' ELSE NULLIF({% date_start date_filter_1 %}, 0) END
           AS timestamp) ;;
  }

  dimension: filter_end_date_1 {
    type: date
    sql: CAST(
          CASE WHEN {% date_end date_filter_1 %} IS NULL THEN '2020-01-01' ELSE NULLIF({% date_end date_filter_1 %}, 0) END
          as timestamp);;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: gmm_id {
    type: number
    sql: ${TABLE}.gmm_id ;;
  }

  dimension: gmm_desc {
    type: string
    sql: ${TABLE}.gmm_desc ;;
  }

  dimension: greg_dt {
    type: date
    sql: cast(${TABLE}.GREG_DT as timestamp) ;;
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
    label: "Price Type Group"
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

  measure: live_product_a {
    type: sum
    sql: ${TABLE}.LiveProductA ;;
  }

  measure: live_product_b {
    type: sum
    sql: ${TABLE}.LiveProductB ;;
  }

  measure: per_var {
    label: "(% VAR) Live Product"
    type: number
    value_format: "0.0\%"
    sql: ((${live_product_a}-${live_product_b})/nullif(${live_product_b},0))*100 ;;
  }

  measure: confirmed_sales_a {
    type: sum
    sql: ${TABLE}.ConfirmedSalesA ;;
  }

  measure: confirmed_sales_b {
    type: sum
    sql: ${TABLE}.ConfirmedSalesB ;;
  }

  measure: per_var1 {
    label: "(% VAR) Confirmed Sales"
    type: number
    value_format: "0.0\%"
    sql: ((${confirmed_sales_a}-${confirmed_sales_b})/nullif(${confirmed_sales_b},0))*100;;
  }

  measure: units_sold_a {
    type: sum
    sql: ${TABLE}.units_soldA ;;
  }

  measure: units_sold_b {
    type: sum
    sql: ${TABLE}.units_soldB ;;
  }

  measure: per_var2 {
    label: "(% VAR) Units Sold"
    type: number
    value_format: "0.0\%"
    sql: ((${units_sold_a}-${units_sold_b})/nullif(${units_sold_b},0))*100  ;;
  }

  measure: aura {
    label: "AUR Period A"
    type: number
    value_format: "$0.00"
    sql: ${confirmed_sales_a}/ NULLIF(${units_sold_a}, 0);;
  }

  measure: aurb {
    label: "AUR Period B"
    type: number
    value_format: "$0.00"
    sql: ${confirmed_sales_b}/ NULLIF(${units_sold_b}, 0);;
  }

  measure: per_var3 {
    label: "(% VAR) AUR"
    type: number
    value_format: "0.0\%"
    sql: ((${aura}-${aurb})/nullif(${aurb},0))*100 ;;
  }

  measure: lst_cost_amt_a {
    type: sum
    sql: ${TABLE}.lst_cost_amt_a ;;
  }

  measure: lst_cost_amt_b {
    type: sum
    sql: ${TABLE}.lst_cost_amt_b ;;
  }

  measure:mmua {
    label: "MMU Period A"
    type: number
    value_format: "0.0\%"
    sql: (((${confirmed_sales_a}/nullif(${units_sold_a},0)) - (${lst_cost_amt_a}/nullif(${units_sold_a},0)))/(${confirmed_sales_a}/nullif(${units_sold_a},0)))*100  ;;
  }

  measure:mmub {
    label: "MMU Period B"
    type: number
    value_format: "0.0\%"
    sql: (((${confirmed_sales_b}/ NULLIF(${units_sold_b}, 0)) - (${lst_cost_amt_b}/ NULLIF(${units_sold_b}, 0)))/(${confirmed_sales_b}/ NULLIF(${units_sold_b}, 0)))*100  ;;
  }

  measure: per_var4 {
    label: "(% VAR) MMU"
    type: number
    value_format: "0.0\%"
    sql: ((${mmua}-${mmub})/nullif(${mmub},0))*100 ;;
  }

  measure:  item_cost_period_a {
    label: "Item Cost Period A"
    type: number
    value_format: "$0.00"
    sql: ${lst_cost_amt_a}/nullif(${units_sold_a},0) ;;
  }

  measure:  item_cost_period_b {
    label: "Item Cost Period B"
    type: number
    value_format: "$0.00"
    sql: ${lst_cost_amt_b}/nullif(${units_sold_b},0) ;;
  }

  measure: per_var5 {
    label: "(% VAR) Item Cost"
    type: number
    value_format: "0.0\%"
    sql:((${item_cost_period_a}-${item_cost_period_b})/nullif(${item_cost_period_b},0))*100 ;;
  }

  measure: AvailTosell_A {
    type: sum
    sql: ${TABLE}.AvailTosell_A ;;
  }

  measure: AvailTosell_B {
    type: sum
    sql: ${TABLE}.AvailTosell_B ;;
  }

  measure: per_var6 {
    label: "(% VAR) Avail to Sell"
    type: number
    value_format: "0.0\%"
    sql: ((${AvailTosell_A}-${AvailTosell_B})/nullif(${AvailTosell_B},0))*100 ;;
  }

  measure: OnOrder_A {
    type: sum
    sql: ${TABLE}.OnOrder_A ;;
  }

  measure: OnOrder_B {
    type: sum
    sql: ${TABLE}.OnOrder_B ;;
  }

  measure: per_var7 {
    label: "(% VAR) On Order"
    type: number
    value_format: "0.0\%"
    sql: ((${OnOrder_A}-${OnOrder_B})/nullif(${OnOrder_B},0))*100 ;;
  }

  measure: four_wk_sls_qty_a {
    type: sum
    sql: ${TABLE}.four_wk_sls_qty_a ;;
  }

  measure: four_wk_sls_qty_b {
    type: sum
    sql: ${TABLE}.four_wk_sls_qty_b ;;
  }

  measure:  sell_through_rate_period_a{
    label: "Sell Through Rate Period A"
    type: number
    value_format: "0.0\%"
    sql: (${four_wk_sls_qty_a} /nullif((${four_wk_sls_qty_a}+${AvailTosell_A}),0))*100 ;;
  }

  measure:  sell_through_rate_period_b{
    label: "Sell Through Rate Period B"
    type: number
    value_format: "0.0\%"
    sql: (${four_wk_sls_qty_b} /nullif((${four_wk_sls_qty_b}+${AvailTosell_B}),0))*100 ;;
  }

  measure: per_var8 {
    label: "(% VAR) Sell Through Rate"
    type: number
    value_format: "0.0\%"
    sql: ((${sell_through_rate_period_a}-${sell_through_rate_period_b})/nullif(${sell_through_rate_period_b},0))*100 ;;
  }

  measure: TotUnitSold_A {
    label: "Tot Unit Sold Std Qty Period A"
    type: sum
    sql: ${TABLE}.TotUnitSold_A ;;
  }

  measure: TotUnitSold_B {
    label: "Tot Unit Sold Std Qty Period B"
    type: sum
    sql: ${TABLE}.TotUnitSold_B ;;
  }

  measure:  per_var9 {
    label: "(% VAR) Tot Unit Sold Std Qty"
    type: number
    value_format: "0.0\%"
    sql:  ((${TotUnitSold_A}-${TotUnitSold_A})/nullif(${TotUnitSold_B},0))*100;;
  }

  measure: StdRtRnUnitQty_A {
    label: "Std Rtrn Unit Qty Period A"
    type: sum
    sql: ${TABLE}.StdRtRnUnitQty_A ;;
  }

  measure: StdRtRnUnitQty_B {
    label: "Std Rtrn Unit Qty Period B"
    type: sum
    sql: ${TABLE}.StdRtRnUnitQty_B ;;
  }

  measure:  per_var10 {
    label: "(% VAR) Std Rtrn Unit Qty"
    type: number
    value_format: "0.0\%"
    sql: ((${StdRtRnUnitQty_A}-${StdRtRnUnitQty_B})/nullif(${StdRtRnUnitQty_B},0))*100  ;;
  }

  measure:  Return_Rate_A{
    label: "Return Rate Period A"
    type: number
    value_format: "0.0\%"
    sql: (${StdRtRnUnitQty_A}/nullif(${TotUnitSold_A},0))*100 ;;
  }

  measure:  Return_Rate_B{
    label: "Return Rate Period B"
    type: number
    value_format: "0.0\%"
    sql: (${StdRtRnUnitQty_B}/nullif(${TotUnitSold_B},0))*100 ;;
  }

  measure:  per_var11 {
    label: "(% VAR) Return Rate"
    type: number
    value_format: "0.0\%"
    sql: ((${Return_Rate_A}-${Return_Rate_B})/nullif(${Return_Rate_B},0))*100  ;;
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
      per_var2
    ]
  }
}
