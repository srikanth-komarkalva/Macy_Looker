view: wip_summary {
  derived_table: {
    sql: WITH container_derived AS (
                          SELECT  e.id AS Id
                                  , e.entity_id AS ContainerNbr
                                  , e.entity_type AS ContainerType
                                  , e.entity_status AS CurrentStatus
                                  , e.created_time AS CreatedTime
                          FROM    `mtech-dc2-prod.inventory.entity` e
                          WHERE   e.entity_type IN ('TOTE', 'CRT', 'BINBOX', 'CSE')
                                  AND e.enabled = 1
                                  AND e.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE ID = e.id AND enabled = 1)
                          )

    , case_receipt_quantity AS    (
                                  SELECT    cea.attribute_value AS RcptNbr
                                            , SUM(ss.quantity) AS RcvdQty
                                            , MIN(e.updated_time) AS EarliestRcvdDatetime
                                  FROM      `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                            INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                                ON e.entity_id = ss.container
                                                AND DATETIME_TRUNC(DATETIME(e.created_time), MINUTE) = DATETIME_TRUNC(DATETIME(ss.created_time), MINUTE)
                                                AND e.entity_status = 'CRE'
                                            INNER JOIN `mtech-dc2-prod.inventory.common_entity_attributes` cea
                                                ON cea.entity_id = e.id
                                                AND cea.attribute_name = 'POReceipt'
                                  WHERE     ss.container_type_id = 6
                                            AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id)
                                            AND cea.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.common_entity_attributes` WHERE id = cea.id)
                                            AND e.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = 'CRE')
                                  GROUP BY  RcptNbr
                                  )

    , tote_receipt_quantity AS    (
                                  SELECT    cea.attribute_value AS RcptNbr
                                            , SUM(ss.quantity) AS RcvdQty
                                            , MIN(e.created_time) AS EarliestRcvdDatetime
                                  FROM      `mtech-dc2-prod.inventory.common_entity_attributes` cea
                                            INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                                ON e.id = cea.entity_id
                                                AND e.entity_type = 'TOTE'
                                            INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                                ON ss.container = e.entity_id
                                                AND DATETIME_TRUNC(DATETIME(ss.created_time), MINUTE) = DATETIME_TRUNC(DATETIME(e.created_time), MINUTE)
                                  WHERE     cea.attribute_name = 'POReceipt'
                                            AND cea.action = 'ADD'
                                            AND e.action = 'ADD'
                                            AND ss.version_id = 0
                                            AND NOT EXISTS (SELECT 1 FROM case_receipt_quantity WHERE RcptNbr = cea.attribute_value)
                                  GROUP BY  RcptNbr
                                  )

    , receipt_quantity AS   (
                            SELECT RcptNbr, RcvdQty, EarliestRcvdDatetime FROM case_receipt_quantity
                            UNION ALL
                            SELECT RcptNbr, RcvdQty, EarliestRcvdDatetime FROM tote_receipt_quantity
                            )

SELECT    CASE WHEN wv.FlowType = 'HAF' THEN 'HAF' WHEN wv.FlowType = 'PMR' THEN 'BKG' WHEN wv.FlowType IS NULL THEN pa.ProcessArea END AS ProcessArea
          , IFNULL(wip.WaveNumber, CAST(po.PoNbr AS STRING)) AS PoNbr
          , IFNULL(wip.WaveNumber, wip.RcptNbr) AS RcptNbr
          , wip.RcvdQty AS RcvdQty
          , SUM(wip.Tkt_Prep_Today) AS Prep_Today
          , SUM(wip.Tkt_Prep_Day1) AS Prep_Day1
          , SUM(wip.Tkt_Prep_Day2) AS Prep_Day2
          , SUM(wip.Tkt_Prep_Day3) AS Prep_Day3
          , SUM(wip.Tkt_Prep_Day4) AS Prep_Day4
          , SUM(wip.Putaway_Today) AS Put_Today
          , SUM(wip.Putaway_Day1) AS Put_Day1
          , SUM(wip.Putaway_Day2) AS Put_Day2
          , SUM(wip.Putaway_Day3) AS Put_Day3
          , SUM(wip.Putaway_Day4) AS Put_Day4
          , SUM(wip.Put_Pack_Today) AS Pack_Today
          , SUM(wip.Put_Pack_Day1) AS Pack_Day1
          , SUM(wip.Put_Pack_Day2) AS Pack_Day2
          , SUM(wip.Put_Pack_Day3) AS Pack_Day3
          , SUM(wip.Put_Pack_Day4) AS Pack_Day4
          , SUM(wip.Pick_Today) AS Pick_Today
          , SUM(wip.Pick_Day1) AS Pick_Day1
          , SUM(wip.Pick_Day2) AS Pick_Day2
          , SUM(wip.Pick_Day3) AS Pick_Day3
          , SUM(wip.Pick_Day4) AS Pick_Day4
          , SUM(wip.Presort_Today) AS Presort_Today
          , SUM(wip.Presort_Day1) AS Presort_Day1
          , SUM(wip.Presort_Day2) AS Presort_Day2
          , SUM(wip.Presort_Day3) AS Presort_Day3
          , SUM(wip.Presort_Day4) AS Presort_Day4
          , SUM(wip.Ship_Today) AS Ship_Today
          , SUM(wip.Ship_Day1) AS Ship_Day1
          , SUM(wip.Ship_Day2) AS Ship_Day2
          , SUM(wip.Ship_Day3) AS Ship_Day3
          , SUM(wip.Ship_Day4) AS Ship_Day4
--          , wip.ContainerNbr AS ContainerNbr
FROM      (
          SELECT    RcptNbr
                    , WaveNumber
                    , LgclLocnNbr
                    , RcvdQty
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus IN ('CRE', 'LCT', 'DST', 'RLS', 'PRT') AND Age = 0 THEN Quantity ELSE 0 END AS Tkt_Prep_Today
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus IN ('CRE', 'LCT', 'DST', 'RLS', 'PRT') AND Age = 1 THEN Quantity ELSE 0 END AS Tkt_Prep_Day1
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus IN ('CRE', 'LCT', 'DST', 'RLS', 'PRT') AND Age = 2 THEN Quantity ELSE 0 END AS Tkt_Prep_Day2
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus IN ('CRE', 'LCT', 'DST', 'RLS', 'PRT') AND Age = 3 THEN Quantity ELSE 0 END AS Tkt_Prep_Day3
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus IN ('CRE', 'LCT', 'DST', 'RLS', 'PRT') AND Age > 3 THEN Quantity ELSE 0 END AS Tkt_Prep_Day4
                    , CASE WHEN ContainerType IN ('BINBOX', 'CSE') AND CurrentStatus IN ('STG', 'SRT', 'SPW', 'CRE', 'LCT') AND Age = 0 THEN Quantity ELSE 0 END AS Putaway_Today
                    , CASE WHEN ContainerType IN ('BINBOX', 'CSE') AND CurrentStatus IN ('STG', 'SRT', 'SPW', 'CRE', 'LCT') AND Age = 1 THEN Quantity ELSE 0 END AS Putaway_Day1
                    , CASE WHEN ContainerType IN ('BINBOX', 'CSE') AND CurrentStatus IN ('STG', 'SRT', 'SPW', 'CRE', 'LCT') AND Age = 2 THEN Quantity ELSE 0 END AS Putaway_Day2
                    , CASE WHEN ContainerType IN ('BINBOX', 'CSE') AND CurrentStatus IN ('STG', 'SRT', 'SPW', 'CRE', 'LCT') AND Age = 3 THEN Quantity ELSE 0 END AS Putaway_Day3
                    , CASE WHEN ContainerType IN ('BINBOX', 'CSE') AND CurrentStatus IN ('STG', 'SRT', 'SPW', 'CRE', 'LCT') AND Age > 3 THEN Quantity ELSE 0 END AS Putaway_Day4
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus = 'VSC' AND Age = 0 THEN Quantity ELSE 0 END AS Put_Pack_Today
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus = 'VSC' AND Age = 1 THEN Quantity ELSE 0 END AS Put_Pack_Day1
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus = 'VSC' AND Age = 2 THEN Quantity ELSE 0 END AS Put_Pack_Day2
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus = 'VSC' AND Age = 3 THEN Quantity ELSE 0 END AS Put_Pack_Day3
                    , CASE WHEN ContainerType = 'TOTE' AND CurrentStatus = 'VSC' AND Age > 3 THEN Quantity ELSE 0 END AS Put_Pack_Day4
                    , CASE WHEN CurrentStatus = 'RSV' AND Age = 0 THEN Quantity ELSE 0 END AS Pick_Today
                    , CASE WHEN CurrentStatus = 'RSV' AND Age = 1 THEN Quantity ELSE 0 END AS Pick_Day1
                    , CASE WHEN CurrentStatus = 'RSV' AND Age = 2 THEN Quantity ELSE 0 END AS Pick_Day2
                    , CASE WHEN CurrentStatus = 'RSV' AND Age = 3 THEN Quantity ELSE 0 END AS Pick_Day3
                    , CASE WHEN CurrentStatus = 'RSV' AND Age > 3 THEN Quantity ELSE 0 END AS Pick_Day4
                    , CASE WHEN CurrentStatus = 'SPK' AND Age = 0 THEN Quantity ELSE 0 END AS PreSort_Today
                    , CASE WHEN CurrentStatus = 'SPK' AND Age = 1 THEN Quantity ELSE 0 END AS PreSort_Day1
                    , CASE WHEN CurrentStatus = 'SPK' AND Age = 2 THEN Quantity ELSE 0 END AS PreSort_Day2
                    , CASE WHEN CurrentStatus = 'SPK' AND Age = 3 THEN Quantity ELSE 0 END AS PreSort_Day3
                    , CASE WHEN CurrentStatus = 'SPK' AND Age > 3 THEN Quantity ELSE 0 END AS PreSort_Day4
                    , CASE WHEN CurrentStatus = 'PCK' AND Age = 0 THEN Quantity ELSE 0 END AS Ship_Today
                    , CASE WHEN CurrentStatus = 'PCK' AND Age = 1 THEN Quantity ELSE 0 END AS Ship_Day1
                    , CASE WHEN CurrentStatus = 'PCK' AND Age = 2 THEN Quantity ELSE 0 END AS Ship_Day2
                    , CASE WHEN CurrentStatus = 'PCK' AND Age = 3 THEN Quantity ELSE 0 END AS Ship_Day3
                    , CASE WHEN CurrentStatus = 'PCK' AND Age > 3 THEN Quantity ELSE 0 END AS Ship_Day4
--                    , ContainerNbr
          FROM      (
                    SELECT    IF(a.attribute_name = 'WaveNumber', NULL, a.attribute_value) AS RcptNbr
                              , IF(a.attribute_name = 'WaveNumber', a.attribute_value, NULL) AS WaveNumber
                              , cd.CurrentStatus
                              , ss.lgcl_locn_nbr AS LgclLocnNbr
                              , ss.Quantity
                              , DATE_DIFF(CURRENT_DATE(), DATE(rq.EarliestRcvdDatetime), DAY) AS Age
                              , rq.RcvdQty
                              , cd.ContainerType
--                              , cd.ContainerNbr
                    FROM      container_derived cd
                              INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                  ON ss.container = cd.ContainerNbr
                              INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                  ON e.entity_id = CAST(ss.id AS STRING)
                                  AND e.version_id = 0
                              INNER JOIN  (
                                          SELECT    cea.entity_id
                                                    , cea.attribute_name
                                                    , cea.attribute_value
                                                    , RANK() OVER (PARTITION BY cea.entity_id ORDER BY attribute_name = 'WaveNumber' DESC) AS AttribRank
                                                    , RANK() OVER (PARTITION BY cea.entity_id, cea.attribute_name ORDER BY version_id DESC) AS VersionRank
                                          FROM      `mtech-dc2-prod.inventory.common_entity_attributes` cea
                                          WHERE     cea.attribute_name IN ('POReceipt', 'WaveNumber')
                                          ) AS a
                                  ON a.entity_id = e.id
                                  AND a.AttribRank = 1
                                  AND a.VersionRank = 1
                              LEFT JOIN receipt_quantity rq
                                  ON rq.RcptNbr = a.attribute_value
                    WHERE     cd.ContainerType = 'CRT'
                              AND cd.CurrentStatus = 'PCK'
                              AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id)

                    UNION ALL

                    SELECT    IF(y.attribute_value IS NOT NULL, NULL, x.attribute_value) AS RcptNbr
                              , y.attribute_value AS WaveNumber
                              , cd.CurrentStatus
                              , ss.lgcl_locn_nbr AS LgclLocnNbr
                              , ss.Quantity
                              , DATE_DIFF(CURRENT_DATE(), DATE(rq.EarliestRcvdDatetime), DAY) AS Age
                              , rq.RcvdQty
                              , cd.ContainerType
--                              , cd.ContainerNbr
                    FROM      container_derived cd
                              INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                  ON ss.container = cd.ContainerNbr
                                  AND DATETIME_TRUNC(DATETIME(ss.created_time), MINUTE) = DATETIME_TRUNC(DATETIME(cd.CreatedTime), MINUTE)
                              LEFT JOIN (
                                        SELECT  cear.entity_id
                                                , cear.attribute_value
                                        FROM    `mtech-dc2-prod.inventory.common_entity_attributes` cear
                                        WHERE   cear.enabled = 1
                                                AND cear.attribute_name = 'POReceipt'
                                                AND cear.version_id = (SELECT MIN(version_id) FROM `mtech-dc2-prod.inventory.common_entity_attributes` WHERE id = cear.id AND enabled = 1 AND attribute_name = 'POReceipt')
                                        ) AS x
                                  ON x.entity_id = cd.Id
                              LEFT JOIN (
                                        SELECT  e.entity_id
                                                , ceaw.attribute_value
                                        FROM    `mtech-dc2-prod.inventory.common_entity_attributes` ceaw
                                                INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                                    ON e.id = ceaw.entity_id
                                        WHERE   ceaw.attribute_name = 'WaveNumber'
                                                AND ceaw.updated_time = (SELECT MAX(updated_time) FROM `mtech-dc2-prod.inventory.common_entity_attributes` WHERE entity_id = ceaw.entity_id AND attribute_name = 'WaveNumber')
                                                AND e.version_id = (SELECT MAX(version_id)  FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id)
                                        ) AS y
                                  ON y.entity_id = CAST(ss.id AS STRING)
                              LEFT JOIN receipt_quantity rq
                                  ON rq.RcptNbr = IFNULL(y.attribute_value, x.attribute_value)
                    WHERE     cd.ContainerType IN ('BINBOX', 'TOTE', 'CSE')
                              AND cd.CurrentStatus NOT IN ('PTW', 'PTS', 'TRB')
                              AND NOT EXISTS (SELECT 1 FROM `mtech-dc2-prod.inventory.entity` WHERE id = cd.Id AND enabled IS NULL)
                              AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id)
                    )
          ) AS wip
          LEFT JOIN (
                      SELECT  DISTINCT CAST(r.rcpt_nbr AS STRING) AS RcptNbr
                              , r.po_nbr AS PoNbr
                              , h.tot_po_ordr_qty AS PoQty
                      FROM    `mtech-dc2-prod.orders.order_rcpt` r
                              INNER JOIN `mtech-dc2-prod.orders.po_hdr` h
                                  ON h.po_nbr = r.po_nbr
                      WHERE   r.last_modified_ts = (SELECT MAX(last_modified_ts) FROM `mtech-dc2-prod.orders.order_rcpt` WHERE rcpt_nbr = r.rcpt_nbr)
                              AND h.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.orders.po_hdr` WHERE po_nbr = h.po_nbr)
                      ) AS po
              ON po.RcptNbr = wip.RcptNbr
          LEFT JOIN (
                    SELECT  DISTINCT h.po_nbr AS PoNbr
                            , t.attr_typ_desc AS ProcessArea
                    FROM    `mtech-dc2-prod.orders.po_hdr` h
                            INNER JOIN `mtech-dc2-prod.orders.po_attr` a
                                ON a.dept = h.dept_nbr
                                AND a.enabled = 1
                            INNER JOIN `mtech-dc2-prod.orders.attr_typ` t
                                ON t.attr_typ_id = a.attr_typ_id
                                AND t.attr_cat_id = 1
                    WHERE   h.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.orders.po_hdr` WHERE po_nbr = h.po_nbr)
                            AND a.last_modified_ts = (SELECT MAX(last_modified_ts) FROM `mtech-dc2-prod.orders.po_attr` WHERE dept = a.dept)
                            AND t.last_modified_ts = (SELECT MAX(last_modified_ts) FROM `mtech-dc2-prod.orders.attr_typ` WHERE attr_typ_id = t.attr_typ_id)
                    ) AS pa
              ON pa.PONbr = po.PONbr
          LEFT JOIN (
                    SELECT  CAST(w.wave_nbr AS STRING) AS WaveNumber
                            , w.flow_type AS FlowType
                            , w.total_qty AS WaveQty
                    FROM    `mtech-dc2-prod.waving.wave` w
                    WHERE   w.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.waving.wave` WHERE wave_nbr = w.wave_nbr)
                    ) AS wv
              ON wv.WaveNumber = wip.WaveNumber
WHERE     wip.RcptNbr IS NOT NULL
GROUP BY  ProcessArea
          , PoNbr
          , RcptNbr
          , RcvdQty
--          , ContainerNbr
 ;;
  }

  measure: count {
    type: count
#     drill_fields: [detail*]
  }

  dimension: process_area {
    type: string
    hidden: yes
    sql: ${TABLE}.ProcessArea ;;
  }

  dimension: po_nbr {
    type: string
    sql: ${TABLE}.PoNbr ;;
  }

  dimension: rcpt_nbr {
    primary_key: yes
    type: string
    sql: ${TABLE}.RcptNbr ;;
  }

  measure: rcvd_qty {
    type: number
    sql: ${TABLE}.RcvdQty ;;
  }

  measure: prep_today {
    type: sum
    sql: ${TABLE}.Prep_Today ;;
  }

  measure: prep_day1 {
    type: sum
    sql: ${TABLE}.Prep_Day1 ;;
  }

  measure: prep_day2 {
    type: sum
    sql: ${TABLE}.Prep_Day2 ;;
  }

  measure: prep_day3 {
    type: sum
    sql: ${TABLE}.Prep_Day3 ;;
  }

  measure: prep_day4 {
    type: sum
    sql: ${TABLE}.Prep_Day4 ;;
  }

  measure: put_today {
    type: sum
    sql: ${TABLE}.Put_Today ;;
  }

  measure: put_day1 {
    type: sum
    sql: ${TABLE}.Put_Day1 ;;
  }

  measure: put_day2 {
    type: sum
    sql: ${TABLE}.Put_Day2 ;;
  }

  measure: put_day3 {
    type: sum
    sql: ${TABLE}.Put_Day3 ;;
  }

  measure: put_day4 {
    type: sum
    sql: ${TABLE}.Put_Day4 ;;
  }

  measure: pack_today {
    type: sum
    sql: ${TABLE}.Pack_Today ;;
  }

  measure: pack_day1 {
    type: sum
    sql: ${TABLE}.Pack_Day1 ;;
  }

  measure: pack_day2 {
    type: sum
    sql: ${TABLE}.Pack_Day2 ;;
  }

  measure: pack_day3 {
    type: sum
    sql: ${TABLE}.Pack_Day3 ;;
  }

  measure: pack_day4 {
    type: sum
    sql: ${TABLE}.Pack_Day4 ;;
  }

  measure: pick_today {
    type: sum
    sql: ${TABLE}.Pick_Today ;;
  }

  measure: pick_day1 {
    type: sum
    sql: ${TABLE}.Pick_Day1 ;;
  }

  measure: pick_day2 {
    type: sum
    sql: ${TABLE}.Pick_Day2 ;;
  }

  measure: pick_day3 {
    type: sum
    sql: ${TABLE}.Pick_Day3 ;;
  }

  measure: pick_day4 {
    type: sum
    sql: ${TABLE}.Pick_Day4 ;;
  }

  measure: presort_today {
    type: sum
    sql: ${TABLE}.Presort_Today ;;
  }

  measure: presort_day1 {
    type: sum
    sql: ${TABLE}.Presort_Day1 ;;
  }

  measure: presort_day2 {
    type: sum
    sql: ${TABLE}.Presort_Day2 ;;
  }

  measure: presort_day3 {
    type: sum
    sql: ${TABLE}.Presort_Day3 ;;
  }

  measure: presort_day4 {
    type: sum
    sql: ${TABLE}.Presort_Day4 ;;
  }

  measure: ship_today {
    type: sum
    sql: ${TABLE}.Ship_Today ;;
  }

  measure: ship_day1 {
    type: sum
    sql: ${TABLE}.Ship_Day1 ;;
  }

  measure: ship_day2 {
    type: sum
    sql: ${TABLE}.Ship_Day2 ;;
  }

  measure: ship_day3 {
    type: sum
    sql: ${TABLE}.Ship_Day3 ;;
  }

  measure: ship_day4 {
    type: sum
    sql: ${TABLE}.Ship_Day4 ;;
  }


  set: detail {
    fields: [
      process_area,
      po_nbr,
      rcpt_nbr,
      prep_today,
      prep_day1,
      prep_day2,
      prep_day3,
      prep_day4,
      put_today,
      put_day1,
      put_day2,
      put_day3,
      put_day4,
      pack_today,
      pack_day1,
      pack_day2,
      pack_day3,
      pack_day4,
      pick_today,
      pick_day1,
      pick_day2,
      pick_day3,
      pick_day4,
      presort_today,
      presort_day1,
      presort_day2,
      presort_day3,
      presort_day4,
      ship_today,
      ship_day1,
      ship_day2,
      ship_day3,
      ship_day4
    ]
  }
}
