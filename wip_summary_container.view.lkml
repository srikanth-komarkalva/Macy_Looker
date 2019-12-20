view: wip_summary_container {
  derived_table: {
    datagroup_trigger: macys_datagroup
#     indexes: ["PoNbr","RcptNbr"]
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

    , entity_container AS (
                            SELECT    id AS EntityId
                                      , entity_id AS Container
                                      , created_time AS CreatedTime
                                      , RANK() OVER (PARTITION BY entity_id ORDER BY id) AS Rank
                            FROM      `mtech-dc2-prod.inventory.entity`
                            WHERE     version_id = 0

                            UNION ALL

                            SELECT    MAX(id) AS EntityId
                                      , entity_id AS Container
                                      , '9999-12-31' AS CreatedTime
                                      , COUNT(DISTINCT id) + 1 AS Rank
                            FROM      `mtech-dc2-prod.inventory.entity`
                            WHERE     version_id = 0
                            GROUP BY  entity_id
                            )

      , entity_inventory AS (
                            SELECT    id AS SnapshotId
                                      , MAX(updated_time) AS LastUpdated
                            FROM      `mtech-dc2-prod.inventory.inventory_snapshot`
                            GROUP BY  id
                            )

      , snapshot_entity_xref AS   (
                                  SELECT    ss.id AS SnapshotId
                                            , ss.container_type_id
                                            , ss.container
                                            , ss.lgcl_locn_nbr
                                            , e.Id as EntityInventoryId
                                            , ec.EntityId AS EntityContainerId
                                            , CASE ceai.attribute_name WHEN 'WaveNumber' THEN ceai.attribute_value ELSE NULL END AS WaveNbr
                                            , CASE ceai.attribute_name WHEN 'POReceipt' THEN ceai.attribute_value ELSE ceac.attribute_value END AS RcptNbr
                                  FROM      `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                            INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                                ON e.entity_id = CAST(ss.id AS STRING)
                                                AND e.entity_type = 'INVN'
                                                AND e.version_id = 0
                                            INNER JOIN entity_inventory ei
                                                ON ei.SnapshotId = ss.id
                                            INNER JOIN entity_container ec
                                                ON ec.Container = ss.container
                                                AND ei.LastUpdated >= TIMESTAMP_SUB(ec.CreatedTime, INTERVAL 2 SECOND)
                                            INNER JOIN entity_container ecn
                                                ON ecn.Container = ec.Container
                                                AND ecn.Rank = ec.Rank + 1
                                                AND ei.LastUpdated < ecn.CreatedTime
                                            LEFT JOIN   (
                                                        SELECT    entity_id
                                                                  , attribute_value
                                                                  , ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY id DESC, version_id DESC) AS RowNbr
                                                        FROM      `mtech-dc2-prod.inventory.common_entity_attributes`
                                                        WHERE     attribute_name = 'POReceipt'
                                                        ) AS ceac
                                                ON ceac.entity_id = ec.EntityId
                                                AND ceac.RowNbr = 1
                                                AND ss.container_type_id IN (4, 5, 6)
                                            LEFT JOIN   (
                                                        SELECT    entity_id
                                                                  , attribute_name
                                                                  , attribute_value
                                                                  , RANK() OVER (PARTITION BY entity_id ORDER BY attribute_name = 'WaveNumber' DESC) AS AttribRank
                                                                  , RANK() OVER (PARTITION BY entity_id, attribute_name ORDER BY version_id DESC) AS VersionRank
                                                        FROM      `mtech-dc2-prod.inventory.common_entity_attributes`
                                                        WHERE     attribute_name IN ('POReceipt', 'WaveNumber')
                                                        ) AS ceai
                                                ON ceai.entity_id = e.id
                                                AND ceai.AttribRank = 1
                                                AND ceai.VersionRank = 1
                                  WHERE     ss.version_id = 0
                                  )

    , receipt_quantity AS   (
                            SELECT    xref.RcptNbr AS RcptNbr
                                      , SUM(ss.quantity) AS RcvdQty
                                      , MIN(e.updated_time) AS EarliestRcvdDatetime
                            FROM      snapshot_entity_xref xref
                                      INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                          ON ss.id = xref.SnapshotId
                                      INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                          ON e.id = xref.EntityContainerId
                                          AND e.entity_status = 'CRE'
                            WHERE     xref.container_type_id IN (4, 6)
                                      AND e.version_id = (SELECT MIN(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = 'CRE')
                                      AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id AND updated_time <= e.updated_time)
                            GROUP BY  xref.RcptNbr

                            UNION ALL

                            SELECT    CAST(wv.wave_nbr AS STRING) AS RcptNbr
                                      , wv.total_qty AS RcvdQty
                                      , wv.created_time AS EarliestRcvdDatetime
                            FROM      `mtech-dc2-prod.waving.wave` wv
                            WHERE     version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.waving.wave` WHERE wave_nbr = wv.wave_nbr)
                            )

SELECT    CASE
              WHEN wv.FlowType = 'HAF' THEN 'HAF'
              WHEN wv.FlowType = 'PMR' THEN 'BKG'
              WHEN wv.FlowType IS NULL THEN
                  CASE wip.LgclLocnNbr
                      WHEN 7254 THEN 'HAF'
                      ELSE pa.ProcessArea
                  END
          END AS ProcessArea
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
          , wip.ContainerNbr AS ContainerNbr
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
                    , ContainerNbr
          FROM      (
                    SELECT    IF(xref.WaveNbr IS NULL, xref.RcptNbr, NULL) AS RcptNbr
                              , xref.WaveNbr AS WaveNumber
                              , cd.CurrentStatus
                              , ss.lgcl_locn_nbr AS LgclLocnNbr
                              , ss.Quantity
                              , DATE_DIFF(CURRENT_DATE(), DATE(rq.EarliestRcvdDatetime), DAY) AS Age
                              , rq.RcvdQty
                              , cd.ContainerType
                              , cd.ContainerNbr
                    FROM      container_derived cd
                              INNER JOIN snapshot_entity_xref xref
                                  ON xref.EntityContainerId = cd.Id
                              INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                  ON ss.id = xref.SnapshotId
                              LEFT JOIN receipt_quantity rq
                                  ON rq.RcptNbr = IFNULL(xref.WaveNbr, xref.RcptNbr)
                    WHERE     cd.ContainerType = 'CRT'
                              AND cd.CurrentStatus = 'PCK'
                              AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id)

                    UNION ALL

                    SELECT    IF(xref.WaveNbr IS NULL, xref.RcptNbr, NULL) AS RcptNbr
                              , xref.WaveNbr AS WaveNumber
                              , cd.CurrentStatus
                              , ss.lgcl_locn_nbr AS LgclLocnNbr
                              , ss.Quantity
                              , DATE_DIFF(CURRENT_DATE(), DATE(rq.EarliestRcvdDatetime), DAY) AS Age
                              , rq.RcvdQty
                              , cd.ContainerType
                              , cd.ContainerNbr
                    FROM      container_derived cd
                              INNER JOIN snapshot_entity_xref xref
                                  ON xref.EntityContainerId = cd.Id
                              INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                  ON ss.id = xref.SnapshotId
                              LEFT JOIN receipt_quantity rq
                                  ON rq.RcptNbr = IFNULL(xref.WaveNbr, xref.RcptNbr)
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
WHERE     wip.WaveNumber IS NOT NULL OR wip.RcptNbr IS NOT NULL
GROUP BY  ProcessArea
          , PoNbr
          , RcptNbr
          , RcvdQty
          , ContainerNbr
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: process_area {
    type: string
    sql: ${TABLE}.ProcessArea ;;
  }

  dimension: process_area_detail {
    label: "Process Area"
    sql:
        case ${process_area}
        WHEN "BTY" THEN "Beauty"
        WHEN "BLK" THEN "Bulk"
        WHEN "GOR" THEN "Gourmet"
        WHEN "FRG" THEN "Fragile"
        WHEN "JWL" THEN "Jewelry"
        WHEN "OVR" THEN "Oversize"
        WHEN "PTC" THEN "Pick To Carton"
        WHEN "SPK" THEN "Store Pack"
        WHEN "BYP" THEN "Bypass"
        WHEN "CDP" THEN "CDP"
        WHEN "OSC" THEN "Open Sort Count"
        WHEN "UNK" THEN "Unknown"
        WHEN "HAF" THEN "Hold & Flow"
        WHEN "BKG" THEN "Backstage"
        END
        ;;
  }

  dimension: po_nbr {
    label: "PO"
    type: string
    sql: ${TABLE}.PoNbr ;;
  }

  dimension: rcpt_nbr {
    label: "Receipt"
    primary_key: yes
    type: string
    sql: ${TABLE}.RcptNbr ;;
  }

  measure: rcvd_qty {
    type: sum
    sql: ${TABLE}.RcvdQty ;;
  }

  measure: prep_today {
    label: "TKT/Prep Today"
    type: sum
    sql: ${TABLE}.Prep_Today ;;
  }

  measure: prep_day1 {
    label: "TKT/Prep Day 1"
    type: sum
    sql: ${TABLE}.Prep_Day1 ;;
  }

  measure: prep_day2 {
    label: "TKT/Prep Day 2"
    type: sum
    sql: ${TABLE}.Prep_Day2 ;;
  }

  measure: prep_day3 {
    label: "TKT/Prep Day 3"
    type: sum
    sql: ${TABLE}.Prep_Day3 ;;
  }

  measure: prep_day4 {
    label: "TKT/Prep Day 4"
    type: sum
    sql: ${TABLE}.Prep_Day4 ;;
  }


  measure: Prep_Total {
    label: "TKT/Prep Pending"
    type: number
    sql: ${prep_day1}+${prep_day2}+${prep_day3}+${prep_day4}+${prep_today} ;;
  }

  measure: Put_Pack_Total {
    label: "Put/Pack Pending"
    type: number
    sql: ${put_day1}+${put_day2}+${put_day3}+${put_day4}+${put_today} ;;
  }

  measure: Pick_Total {
    label: "Pick Pending"
    type: number
    sql: ${pick_day1}+${pick_day2}+${pick_day3}+${pick_day4}+${pick_today} ;;
  }

  measure: PreSort_Total {
    label: "PerSort Pending"
    type: number
    sql: ${presort_day1}+${presort_day2}+${presort_day3}+${presort_day4}+${presort_today} ;;
  }

  measure: Ship_Total {
    label: "Shipped Pending"
    type: number
    sql: ${ship_day1}+${ship_day2}+${ship_day3}+${ship_day4}+${ship_today} ;;
  }

  measure: put_today {
    label: "Put/Pack Today"
    type: sum
    sql: ${TABLE}.Put_Today ;;
  }


  measure: put_day1 {
    label: "Put/Pack Day 1"
    type: sum
    sql: ${TABLE}.Put_Day1 ;;
  }

  measure: put_day2 {
    label: "Put/Pack Day 2"
    type: sum
    sql: ${TABLE}.Put_Day2 ;;
  }

  measure: put_day3 {
    label: "Put/Pack Day 3"
    type: sum
    sql: ${TABLE}.Put_Day3 ;;
  }

  measure: put_day4 {
    label: "Put/Pack Day 4"
    type: sum
    sql: ${TABLE}.Put_Day4 ;;
  }

  measure: pack_today {
    label: "Pack Today"
    type: sum
    sql: ${TABLE}.Pack_Today ;;
  }

  measure: pack_day1 {
    label: "Pack Day 1"
    type: sum
    sql: ${TABLE}.Pack_Day1 ;;
  }

  measure: pack_day2 {
    label: "Pack Day 2"
    type: sum
    sql: ${TABLE}.Pack_Day2 ;;
  }

  measure: pack_day3 {
    label: "Pack Day 3"
    type: sum
    sql: ${TABLE}.Pack_Day3 ;;
  }

  measure: pack_day4 {
    label: "Pack Day 4"
    type: sum
    sql: ${TABLE}.Pack_Day4 ;;
  }

  measure: pick_today {
    label: "Pick Today"
    type: sum
    sql: ${TABLE}.Pick_Today ;;
  }

  measure: pick_day1 {
    label: "Pick Day 1"
    type: sum
    sql: ${TABLE}.Pick_Day1 ;;
  }

  measure: pick_day2 {
    label: "Pick Day 2"
    type: sum
    sql: ${TABLE}.Pick_Day2 ;;
  }

  measure: pick_day3 {
    label: "Pick Day 3"
    type: sum
    sql: ${TABLE}.Pick_Day3 ;;
  }

  measure: pick_day4 {
    label: "Pick Day 4"
    type: sum
    sql: ${TABLE}.Pick_Day4 ;;
  }

  measure: presort_today {
    label: "PreSort Today"
    type: sum
    sql: ${TABLE}.Presort_Today ;;
  }

  measure: presort_day1 {
    label: "PreSort Day 1"
    type: sum
    sql: ${TABLE}.Presort_Day1 ;;
  }

  measure: presort_day2 {
    label: "PreSort Day 2"
    type: sum
    sql: ${TABLE}.Presort_Day2 ;;
  }

  measure: presort_day3 {
    label: "PreSort Day 3"
    type: sum
    sql: ${TABLE}.Presort_Day3 ;;
  }

  measure: presort_day4 {
    label: "PreSort Day 4"
    type: sum
    sql: ${TABLE}.Presort_Day4 ;;
  }

  measure: ship_today {
    label: "Shipped Today"
    type: sum
    sql: ${TABLE}.Ship_Today ;;
  }

  measure: ship_day1 {
    label: "Shipped Day 1"
    type: sum
    sql: ${TABLE}.Ship_Day1 ;;
  }

  measure: ship_day2 {
    label: "Shipped Day 2"
    type: sum
    sql: ${TABLE}.Ship_Day2 ;;
  }

  measure: ship_day3 {
    label: "Shipped Day 3"
    type: sum
    sql: ${TABLE}.Ship_Day3 ;;
  }

  measure: ship_day4 {
    label: "Shipped Day 4"
    type: sum
    sql: ${TABLE}.Ship_Day4 ;;
  }

  dimension: container_nbr {
    label: "Container Nbr"
    type: string
    sql: ${TABLE}.ContainerNbr ;;
  }

  set: detail {
    fields: [
      process_area,
      po_nbr,
      rcpt_nbr,
      rcvd_qty,
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
      ship_day4,
      container_nbr
    ]
  }
}
