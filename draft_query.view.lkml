view: draft_query {
  derived_table: {
    sql: with Table1 as (
      select  brnd_nm,prd.mdse_dept_nbr,mdse_dept_desc,buyer_desc,mdse_divn_mgr_desc,parent_mdse_divn_desc,gmm_desc,rpt_date.greg_dt ,
      PRD.web_prod_id AS PRDID,Prod_desc as Proddesc,brnd_nm as Brand,prod_typ_desc as Product_Type,
      sum(VIEW_SESSN_PROD_CNT) AS VIEW_SESSN_PROD_CNT,
      SUM(TOT_SLS_AMT) AS TOT_SLS_AMT,
      SUM(ITEM_QTY) AS ITEM_QTY,
      SUM(VIEW_SESSN_CNT) AS VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) AS BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      /*sum(prod_age_nbr) as prod_age_nbr*/
      0 as prod_age_nbr,
      0 as rtng_nbr,
      0 AS RVWS_CNT,
      0 AS STD_RTRN_QTY,
      0 AS STD_SLS_QTY,
      0 FOUR_WK_SLS_QTY,
      0 AS AVAIL_TO_SELL_QTY,
      0 AS oo_qty
      from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.pdp_summ_v` summary
      INNER JOIN `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` prd on summary.WEB_PROD_ID = PRD.WEB_PROD_ID
      inner join `mtech-daas-reference-pdata-dev.rfnd_ref_v.cognos_rpt_date` rpt_date on  summary.GREG_DT =  rpt_date.GREG_DT
      INNER JOIN `mtech-daas-reference-pdata-dev.rfnd_ref_v.curr_rpt_date` cur_date ON rpt_date.CURR_DT_KEY = cur_date.CURR_DT_KEY
      WHERE
       Coalesce(Page_Typ_Cd,'Unknown') <> 'Master' AND (GMM_ID > 0 and GMM_ID <> 7) AND PRD.OPER_DIVN_NBR=12 -- filters from cube
      --and rpt_date.GREG_DT BETWEEN '2019-10-01' AND '2019-10-10' ---- mandatory report filter Period A
      AND {% condition greg_dt %} >= filter_start_date {% endcondition %}
      AND {% condition greg_dt %} <= filter_end_date {% endcondition %}
      --and prd.brnd_nm='Lee' -- optional report prompt
      group by brnd_nm,mdse_dept_nbr,mdse_dept_desc,buyer_desc,mdse_divn_mgr_desc,parent_mdse_divn_desc,gmm_desc,rpt_date.greg_dt,
      PRD.web_prod_id ,Prod_desc ,brnd_nm,prod_typ_desc

      union all

      select  brnd_nm,prd.mdse_dept_nbr,mdse_dept_desc,buyer_desc,mdse_divn_mgr_desc,parent_mdse_divn_desc,gmm_desc,rpt_date.greg_dt ,
      PRD.web_prod_id AS PRDID,Prod_desc as Proddesc,brnd_nm as Brand,prod_typ_desc as Product_Type,
      0 AS VIEW_SESSN_PROD_CNT,
      0 AS TOT_SLS_AMT,
      0 AS ITEM_QTY,
      0 AS VIEW_SESSN_CNT,
      0 AS BUY_SESSN_CNT,
      0 As SHOP_SESSN_CNT,
      0 as LST_COST_AMT,
      0 as prod_age_nbr,
      /*Sum(rtng_nbr) rtng_nbr*/
      0 as rtng_nbr ,
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
      --and rpt_date.GREG_DT = '2019-10-10' ---- mandatory report filter Period A
      and {% condition greg_dt %} = filter_end_date {% endcondition %}
      --and prd.brnd_nm='Lee' -- optional report prompt
      group by  brnd_nm,mdse_dept_nbr,mdse_dept_desc,buyer_desc,
      mdse_divn_mgr_desc,
      parent_mdse_divn_desc,
      gmm_desc,rpt_date.greg_dt,PRD.web_prod_id ,Prod_desc ,brnd_nm,prod_typ_desc
      )


      select Sno,PRDID,Proddesc,Brand,Product_Type, mdse_dept_nbr,greg_dt,rank,
      sum(Confirmed_Sales) as Confirmed_Sales,
      sum(units_Sold) as units_Sold,
      sum(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      sum(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      sum(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      sum(LST_COST_AMT) as LST_COST_AMT,
      sum(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,
      sum(Avail_to_Sell) as Avail_to_Sell,
      sum(On_Order) as On_Order,
      sum(age) as age,
      sum(Tot_Unit_Sold_Std_Qty) as Tot_Unit_Sold_Std_Qty,
      sum(Std_Rtrn_Unit_Qty) as Std_Rtrn_Unit_Qty,
      sum(Product_Rating) as Product_Rating,
      sum(Number_of_Reviews) as Number_of_Reviews
      from(
      select 1 as Sno,cast(PRDID as string) as PRDID ,Proddesc as Proddesc,Brand as Brand, Product_Type as Product_Type, mdse_dept_nbr,
      greg_dt,rank() over(PARTITION BY Proddesc,Brand,Product_Type,mdse_dept_nbr,greg_dt order by sum(TOT_SLS_AMT) desc) as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      SUM(ITEM_QTY) AS units_Sold,
      SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where mdse_dept_desc  =(select distinct mdse_dept_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where   {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %} )
      and  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %}
      group by PRDID,Proddesc,Brand,Product_Type ,mdse_dept_nbr,greg_dt)
      where {% condition rank %} rank {% endcondition %}
      group by Sno,PRDID,Proddesc,Brand,Product_Type, mdse_dept_nbr,greg_dt,rank

       union all

      select  2 as Sno, '' as PRDID ,'' as Proddesc,'' as Brand, mdse_dept_desc as Product_Type, mdse_dept_nbr,greg_dt,null as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      SUM(ITEM_QTY) AS units_Sold,
      SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where mdse_dept_desc  =(select distinct mdse_dept_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where    {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %} )
      and  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %}
      group by Product_Type ,mdse_dept_nbr,greg_dt

      union all

      select 3 as Sno,'' as PRDID ,'' as Proddesc,'' as Brand,buyer_desc as  Product_Type,mdse_dept_nbr,greg_dt,null as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      SUM(ITEM_QTY) AS units_Sold,
       SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,

      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,

      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,

      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where  buyer_desc  =(select distinct buyer_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where   {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %} )
      and  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %}
      group by Product_Type ,mdse_dept_nbr,greg_dt

      union all

      select 4 as Sno,'' as PRDID ,'' as Proddesc,'' as Brand,mdse_divn_mgr_desc as  Product_Type,mdse_dept_nbr,greg_dt,null as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,

      SUM(ITEM_QTY) AS units_Sold,
       SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,

      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,

      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,

      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where  buyer_desc  =(select distinct buyer_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where   {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %} )
      and  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %}
      group by Product_Type ,mdse_dept_nbr,greg_dt

      union all

      select 5 as Sno,'' as PRDID ,'' as Proddesc,'' as Brand,parent_mdse_divn_desc as  Product_Type,mdse_dept_nbr,greg_dt,null as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,

      SUM(ITEM_QTY) AS units_Sold,
       SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,

      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,

      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,

      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where  buyer_desc  =(select distinct buyer_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where   {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %} )
      and  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %}
      group by Product_Type ,mdse_dept_nbr,greg_dt

       union all

      select 6 as Sno,'' as PRDID ,'' as Proddesc,'' as Brand,gmm_desc as  Product_Type,mdse_dept_nbr,greg_dt,null as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,

      SUM(ITEM_QTY) AS units_Sold,
       SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,

      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,

      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,

      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where  buyer_desc  =(select distinct buyer_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where   {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %} )
      and  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %}
      group by Product_Type ,mdse_dept_nbr,greg_dt

       union all

      select 7 as Sno,'' as PRDID ,'' as Proddesc,'' as Brand,'ALL' as  Product_Type,mdse_dept_nbr,greg_dt,null as rank,
      sum(TOT_SLS_AMT) as Confirmed_Sales,

      SUM(ITEM_QTY) AS units_Sold,
       SUM(VIEW_SESSN_CNT) as VIEW_SESSN_CNT,
      SUM(BUY_SESSN_CNT) as BUY_SESSN_CNT,
      SUM(SHOP_SESSN_CNT) as SHOP_SESSN_CNT,
      SUM(LST_COST_AMT) as LST_COST_AMT,
      SUM(FOUR_WK_SLS_QTY) as FOUR_WK_SLS_QTY,

      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,

      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,

      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      where  buyer_desc  =(select distinct buyer_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v`
      where  {% condition mdse_dept_nbr %} mdse_dept_nbr {% endcondition %})
      group by mdse_dept_nbr,greg_dt
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  filter: date_filter {
    description: "Use this date filter in combination with the timeframes dimension for dynamic date filtering"
    type: date_time
    sql: {% condition date_filter %} cast(${TABLE}.greg_dt as timestamp) {% endcondition %} ;;
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


  dimension: sno {
    type: number
    sql: ${TABLE}.Sno ;;
  }

  dimension: prdid {
    type: string
    sql: ${TABLE}.PRDID ;;
  }

  dimension: proddesc {
    type: string
    sql: ${TABLE}.Proddesc ;;
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.Brand ;;
  }

  dimension: product_type {
    type: string
    sql: ${TABLE}.Product_Type ;;
  }

  dimension: mdse_dept_nbr {
    type: number
    primary_key: yes
    sql: ${TABLE}.mdse_dept_nbr ;;
  }

  dimension: greg_dt {
    type: date
    sql: ${TABLE}.greg_dt ;;
  }

  dimension: rank {
    type: number
    sql: ${TABLE}.rank ;;
  }

#   dimension: sequence {
#     type: number
#     sql: ${TABLE}.Sequence ;;
#   }

  measure: confirmed_sales {
    type: sum
    sql: ${TABLE}.Confirmed_Sales ;;
  }

  measure: aura {
    label: "AUR"
    type: number
    value_format: "$0.00"
    sql: NULLIF(${confirmed_sales},0)/NULLIF(${units_sold},0);;
  }

  measure: units_sold {
    type: sum
    sql: ${TABLE}.units_Sold ;;
  }

  measure: productivity {
    type: number
    value_format: "$0.00"
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
    sql: NULLIF(${shop_sessn_cnt},0)/NULLIF(${view_sessn_cnt},0) ;;
  }

  measure: checkout_conv {
    label: "Checkout Conv"
    type: number
    value_format: "0.0\%"
    sql: NULLIF(${buy_sessn_cnt},0)/NULLIF(${shop_sessn_cnt},0) ;;
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

  measure: view_sessn_cnt {
    type: sum
    sql: ${TABLE}.VIEW_SESSN_CNT ;;
  }

  measure: buy_sessn_cnt {
    type: sum
    sql: ${TABLE}.BUY_SESSN_CNT ;;
  }

  measure: shop_sessn_cnt {
    type: number
    sql: ${TABLE}.SHOP_SESSN_CNT ;;
  }

  measure: lst_cost_amt {
    type: sum
    sql: ${TABLE}.LST_COST_AMT ;;
  }

  measure: four_wk_sls_qty {
    type: sum
    sql: ${TABLE}.FOUR_WK_SLS_QTY ;;
  }

  measure: avail_to_sell {
    type: sum
    sql: ${TABLE}.Avail_to_Sell ;;
  }

  measure: on_order {
    type: sum
    sql: ${TABLE}.On_Order ;;
  }

  measure: age {
    type: sum
    sql: ${TABLE}.age ;;
  }

  measure: sell_through_rate_a {
    label: "Sell Through Rate"
    type: number
    value_format: "0.00\%"
    #sql:  ${four_wk_sls_qty}/(NULLIF(${four_wk_sls_qty},0) + ${avail_to_sell})*100 ;;
    sql:  (NULLIF(${four_wk_sls_qty},0)/NULLIF(${avail_to_sell},0))*100;;
  }

  measure: tot_unit_sold_std_qty {
    type: sum
    sql: ${TABLE}.Tot_Unit_Sold_Std_Qty ;;
  }

  measure: std_rtrn_unit_qty {
    type: sum
    value_format: "(#,##0)"
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

  set: detail {
    fields: [
      sno,
      prdid,
      proddesc,
      brand,
      product_type,
      mdse_dept_nbr,
      greg_dt,
      rank,
      confirmed_sales,
      units_sold,
      view_sessn_cnt,
      buy_sessn_cnt,
      shop_sessn_cnt,
      lst_cost_amt,
      four_wk_sls_qty,
      avail_to_sell,
      on_order,
      age,
      tot_unit_sold_std_qty,
      std_rtrn_unit_qty,
      product_rating,
      number_of_reviews
    ]
  }
}
