view: pdp_totals {
  derived_table: {
    sql: with Table1 as (
      select  brnd_nm,prd.mdse_dept_nbr,mdse_dept_desc,buyer_desc,mdse_divn_mgr_desc,parent_mdse_divn_desc,gmm_desc,rpt_date.GREG_DT ,
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
      and rpt_date.GREG_DT BETWEEN '2019-01-01' AND '2019-12-31' ---- mandatory report filter Period A
      --and prd.brnd_nm='Lee' -- optional report prompt
      group by brnd_nm,mdse_dept_nbr,mdse_dept_desc,buyer_desc,mdse_divn_mgr_desc,parent_mdse_divn_desc,gmm_desc,rpt_date.GREG_DT

      union all

      select  brnd_nm,prd.mdse_dept_nbr,mdse_dept_desc,buyer_desc,mdse_divn_mgr_desc,parent_mdse_divn_desc,gmm_desc,rpt_date.GREG_DT ,
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
      and rpt_date.GREG_DT = '2019-12-31' ---- mandatory report filter Period A
      --and prd.brnd_nm='Lee' -- optional report prompt
      group by  brnd_nm,mdse_dept_nbr,mdse_dept_desc,buyer_desc,
      mdse_divn_mgr_desc,
      parent_mdse_divn_desc,
      gmm_desc,rpt_date.GREG_DT
      )

      select  1 as SLNO,'Dept' as Level, mdse_dept_desc as  caption, mdse_dept_nbr,
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
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      --where mdse_dept_desc  =(select distinct mdse_dept_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` where mdse_dept_nbr=134)
      group by caption,mdse_dept_nbr

      union all

      select 2,'Buyer',buyer_desc as  caption, mdse_dept_nbr,
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
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      --where  buyer_desc  =(select distinct buyer_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` where mdse_dept_nbr=134)
      group by caption,mdse_dept_nbr

      union all

      select 3,'Div',mdse_divn_mgr_desc as  caption,mdse_dept_nbr,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      --case when SUM(ITEM_QTY) <>0 then SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) else 0 end AS AUR,
      SUM(ITEM_QTY) AS units_Sold,

      --case when SUM(VIEW_SESSN_CNT)<>0 then  SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) else 0 end AS Productivity,
      --case when SUM(VIEW_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end AS View_to_Buy_Conv,
      --case when SUM(VIEW_SESSN_CNT) <>0 then (SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end  AS Add_to_Bag_Conv,
      --case when SUM(SHOP_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 else 0 end  AS Checkout_Conv,
      --case when SUM(ITEM_QTY)<>0 then (((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 else 0 end AS MMU,
      --case when SUM(ITEM_QTY)<>0 then sum(LST_COST_AMT)/SUM(ITEM_QTY) else 0 end as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      --case when SUM(FOUR_WK_SLS_QTY)<>0 then (SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 else 0 end AS Sell_Through_Rate,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      --case when SUM(STD_SLS_QTY)<>0 then (ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 else 0 end AS Return_Rate,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews from Table1
      --where  mdse_divn_mgr_desc  =(select distinct mdse_divn_mgr_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` where mdse_dept_nbr=134)
      group by caption,mdse_dept_nbr

      union all
      select 4,'PDIV',parent_mdse_divn_desc as  caption, mdse_dept_nbr,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      --case when SUM(ITEM_QTY) <>0 then SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) else 0 end AS AUR,
      SUM(ITEM_QTY) AS units_Sold,
      --case when SUM(VIEW_SESSN_CNT)<>0 then  SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) else 0 end AS Productivity,
      --case when SUM(VIEW_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end AS View_to_Buy_Conv,
      --case when SUM(VIEW_SESSN_CNT) <>0 then (SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end  AS Add_to_Bag_Conv,
      --case when SUM(SHOP_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 else 0 end  AS Checkout_Conv,
      --case when SUM(ITEM_QTY)<>0 then (((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 else 0 end AS MMU,
      --case when SUM(ITEM_QTY)<>0 then sum(LST_COST_AMT)/SUM(ITEM_QTY) else 0 end as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      --case when SUM(FOUR_WK_SLS_QTY)<>0 then (SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 else 0 end AS Sell_Through_Rate,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      --case when SUM(STD_SLS_QTY)<>0 then (ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 else 0 end AS Return_Rate,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews
      from Table1
      --where    parent_mdse_divn_desc  =(select distinct parent_mdse_divn_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` where mdse_dept_nbr=134)

      group by caption,mdse_dept_nbr


      union all
      select  5,'GMM',gmm_desc  as  caption, mdse_dept_nbr,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      --case when SUM(ITEM_QTY) <>0 then SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) else 0 end AS AUR,
      SUM(ITEM_QTY) AS units_Sold,
      --case when SUM(VIEW_SESSN_CNT)<>0 then  SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) else 0 end AS Productivity,
      --case when SUM(VIEW_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end AS View_to_Buy_Conv,
      --case when SUM(VIEW_SESSN_CNT) <>0 then (SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end  AS Add_to_Bag_Conv,
      --case when SUM(SHOP_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 else 0 end  AS Checkout_Conv,
      --case when SUM(ITEM_QTY)<>0 then (((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 else 0 end AS MMU,
      --case when SUM(ITEM_QTY)<>0 then sum(LST_COST_AMT)/SUM(ITEM_QTY) else 0 end as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      --case when SUM(FOUR_WK_SLS_QTY)<>0 then (SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 else 0 end AS Sell_Through_Rate,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      --case when SUM(STD_SLS_QTY)<>0 then (ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 else 0 end AS Return_Rate,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews
      from Table1
      --where   gmm_desc  =(select distinct gmm_desc from `mtech-daas-product-pdata-dev.rfnd_prod_mcy_v.curr_prod_dim_v` where mdse_dept_nbr=134)
      group by caption,mdse_dept_nbr

      union all
      select  6,'All','All'  as  caption, mdse_dept_nbr,
      sum(TOT_SLS_AMT) as Confirmed_Sales,
      --case when SUM(ITEM_QTY) <>0 then SUM(TOT_SLS_AMT)/SUM(ITEM_QTY) else 0 end AS AUR,
      SUM(ITEM_QTY) AS units_Sold,
      --case when SUM(VIEW_SESSN_CNT)<>0 then  SUM(TOT_SLS_AMT)/SUM(VIEW_SESSN_CNT) else 0 end AS Productivity,
      --case when SUM(VIEW_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end AS View_to_Buy_Conv,
      --case when SUM(VIEW_SESSN_CNT) <>0 then (SUM(SHOP_SESSN_CNT)/SUM(VIEW_SESSN_CNT))*100 else 0 end  AS Add_to_Bag_Conv,
      --case when SUM(SHOP_SESSN_CNT)<>0 then (SUM(BUY_SESSN_CNT)/SUM(SHOP_SESSN_CNT) )*100 else 0 end  AS Checkout_Conv,
      --case when SUM(ITEM_QTY)<>0 then (((SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)) - (SUM(LST_COST_AMT)/ SUM(ITEM_QTY)))/(SUM(TOT_SLS_AMT)/SUM(ITEM_QTY)))*100 else 0 end AS MMU,
      --case when SUM(ITEM_QTY)<>0 then sum(LST_COST_AMT)/SUM(ITEM_QTY) else 0 end as Item_cost,
      SUM(AVAIL_TO_SELL_QTY) AS Avail_to_Sell,
      sum(oo_qty) AS On_Order,
      sum(prod_age_nbr) as age,
      --case when SUM(FOUR_WK_SLS_QTY)<>0 then (SUM(FOUR_WK_SLS_QTY)/(SUM(FOUR_WK_SLS_QTY)+SUM(AVAIL_TO_SELL_QTY)))*100 else 0 end AS Sell_Through_Rate,
      SUM(STD_SLS_QTY) AS Tot_Unit_Sold_Std_Qty,
      SUM(STD_RTRN_QTY) AS Std_Rtrn_Unit_Qty,
      --case when SUM(STD_SLS_QTY)<>0 then (ABS((SUM(STD_RTRN_QTY))/SUM(STD_SLS_QTY)))*100 else 0 end AS Return_Rate,
      sum(rtng_nbr) as Product_Rating,
      SUM(RVWS_CNT) AS Number_of_Reviews
      from Table1
      group by mdse_dept_nbr
      order by slno
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: slno {
    type: number
    sql: ${TABLE}.SLNO ;;
  }

  dimension: level {
    type: string
    sql: ${TABLE}.Level ;;
  }

  dimension: caption {
    type: string
    sql: ${TABLE}.caption ;;
  }

  dimension: mdse_dept_nbr {
    type: number
    sql: ${TABLE}.mdse_dept_nbr ;;
  }

  dimension: confirmed_sales {
    type: number
    sql: ${TABLE}.Confirmed_Sales ;;
  }

  dimension: units_sold {
    type: number
    sql: ${TABLE}.units_Sold ;;
  }

  dimension: avail_to_sell {
    type: number
    sql: ${TABLE}.Avail_to_Sell ;;
  }

  dimension: on_order {
    type: number
    sql: ${TABLE}.On_Order ;;
  }

  dimension: age {
    type: number
    sql: ${TABLE}.age ;;
  }

  dimension: tot_unit_sold_std_qty {
    type: number
    sql: ${TABLE}.Tot_Unit_Sold_Std_Qty ;;
  }

  dimension: std_rtrn_unit_qty {
    type: number
    sql: ${TABLE}.Std_Rtrn_Unit_Qty ;;
  }

  dimension: product_rating {
    type: number
    sql: ${TABLE}.Product_Rating ;;
  }

  dimension: number_of_reviews {
    type: number
    sql: ${TABLE}.Number_of_Reviews ;;
  }

  set: detail {
    fields: [
      slno,
      level,
      caption,
      mdse_dept_nbr,
      confirmed_sales,
      units_sold,
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
