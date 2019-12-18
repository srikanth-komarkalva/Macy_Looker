include: "BDA_Reports.model.lkml"

view: bda_data {
  label: "BDA Reports"
  derived_table: {
    datagroup_trigger: macys_datagroup
    partition_keys: ["ShiftDate"]
    cluster_keys: ["ProcessArea","PoNbr"]
    sql: WITH  shiftTimings AS   (
                        SELECT  StartTime
                                , DurationInSec
                                , DayOfWeek
                                , Name
                        FROM    UNNEST(
                                       [STRUCT('04:30:00' AS StartTime, 43199 AS DurationInSec, 2 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('16:30:00' AS StartTime, 43199 AS DurationInSec, 2 AS DayOfWeek, 'SHIFT2' AS Name)
                                      , STRUCT('04:30:00' AS StartTime, 43199 AS DurationInSec, 3 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('16:30:00' AS StartTime, 43199 AS DurationInSec, 3 AS DayOfWeek, 'SHIFT2' AS Name)
                                      , STRUCT('04:30:00' AS StartTime, 43199 AS DurationInSec, 4 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('16:30:00' AS StartTime, 43199 AS DurationInSec, 4 AS DayOfWeek, 'SHIFT2' AS Name)
                                      , STRUCT('04:30:00' AS StartTime, 43199 AS DurationInSec, 5 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('16:30:00' AS StartTime, 43199 AS DurationInSec, 5 AS DayOfWeek, 'SHIFT2' AS Name)
                                      , STRUCT('04:30:00' AS StartTime, 52199 AS DurationInSec, 6 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('19:00:00' AS StartTime, 34199 AS DurationInSec, 6 AS DayOfWeek, 'SHIFT2' AS Name)
                                      , STRUCT('04:30:00' AS StartTime, 52199 AS DurationInSec, 7 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('19:00:00' AS StartTime, 34199 AS DurationInSec, 7 AS DayOfWeek, 'SHIFT2' AS Name)
                                      , STRUCT('04:30:00' AS StartTime, 52199 AS DurationInSec, 1 AS DayOfWeek, 'SHIFT1' AS Name)
                                      , STRUCT('19:00:00' AS StartTime, 34199 AS DurationInSec, 1 AS DayOfWeek, 'SHIFT2' AS Name)
                                      ])
                        )

      , shifts AS (
                  SELECT  days AS ShiftDate
                          , TIMESTAMP(CONCAT(CAST(days AS STRING), ' ', shiftTimings.StartTime), 'America/New_York') AS ShiftStartDatetime
                          , TIMESTAMP(DATETIME_ADD(CAST(CONCAT(CAST(days AS STRING), ' ', shiftTimings.StartTime) AS DATETIME), INTERVAL shiftTimings.DurationInSec SECOND), 'America/New_York') AS ShiftEndDatetime
                          , shiftTimings.Name AS ShiftName
                  FROM    UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 31 DAY), CURRENT_DATE('America/New_York'))) AS days
                          INNER JOIN shiftTimings
                              ON EXTRACT(DAYOFWEEK FROM days) = shiftTimings.DayOfWeek
                  )

      , entity_container AS (
                            SELECT    id AS EntityId
                                      , entity_id AS Container
                                      , CASE entity_type WHEN 'CSE' THEN 6 WHEN 'TOTE' THEN 4 WHEN 'BINBOX' THEN 5 WHEN 'CRT' THEN 1 ELSE 0 END AS ContainerTypeId
                                      , created_time AS CreatedTime
                                      , RANK() OVER (PARTITION BY entity_id, entity_type ORDER BY id) AS Rank
                            FROM      `mtech-dc2-prod.inventory.entity`
                            WHERE     version_id = 0

                            UNION ALL

                            SELECT    MAX(id) AS EntityId
                                      , entity_id AS Container
                                      , CASE entity_type WHEN 'CSE' THEN 6 WHEN 'TOTE' THEN 4 WHEN 'BINBOX' THEN 5 WHEN 'CRT' THEN 1 ELSE 0 END AS ContainerTypeId
                                      , '9999-12-31' AS CreatedTime
                                      , COUNT(DISTINCT id) + 1 AS Rank
                            FROM      `mtech-dc2-prod.inventory.entity`
                            WHERE     version_id = 0
                            GROUP BY  entity_id, entity_type
                            )

      , entity_inventory AS (
                            SELECT    id AS SnapshotId
                                      , MAX(updated_time) AS LastUpdated
                            FROM      `mtech-dc2-prod.inventory.inventory_snapshot`
                        --    WHERE     created_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
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
                                                AND ec.ContainerTypeId = ss.container_type_id
                                                AND ei.LastUpdated >= TIMESTAMP_SUB(ec.CreatedTime, INTERVAL 2 SECOND)
                                            INNER JOIN entity_container ecn
                                                ON ecn.Container = ec.Container
                                                AND ecn.ContainerTypeId = ec.ContainerTypeId
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
                                                                  , RANK() OVER (PARTITION BY entity_id, attribute_name ORDER BY id DESC, version_id DESC) AS VersionRank
                                                        FROM      `mtech-dc2-prod.inventory.common_entity_attributes`
                                                        WHERE     attribute_name IN ('POReceipt', 'WaveNumber')
                                                        ) AS ceai
                                                ON ceai.entity_id = e.id
                                                AND ceai.AttribRank = 1
                                                AND ceai.VersionRank = 1
                                  WHERE     /*ss.created_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
                                            AND */ss.version_id = 0
                                  )

      , container_derived AS  (
                              SELECT  s.ShiftDate AS ShiftDate
                                      , s.ShiftName AS ShiftName
                                      , s.ShiftStartDateTime AS ShiftStartDatetime
                                      , s.ShiftEndDatetime AS ShiftEndDatetime
                                      , e.updated_time AS TransactionTime
                                      , e.id AS Id
                                      , e.entity_id AS ContainerNbr
                                      , e.entity_type AS ContainerType
                                      , e.entity_status AS ContainerStatus
                                      , e.created_time AS CreatedTime
                                      , ROW_NUMBER() OVER(ORDER BY e.id, e.entity_status) AS RowNbr
                              FROM    `mtech-dc2-prod.inventory.entity` e
                                      INNER JOIN shifts s
                                          ON e.updated_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime
                              WHERE   e.entity_type IN ('TOTE', 'CRT')
                                      AND e.entity_status IN ('PRT', 'VSC', 'MFT', 'SHP')
                                      AND e.version_id = (SELECT MIN(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = e.entity_status AND updated_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime)
                                      AND NOT EXISTS (SELECT 1 FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = e.entity_status AND updated_time < s.ShiftStartDatetime)

                              UNION ALL

                              SELECT  s.ShiftDate AS ShiftDate
                                      , s.ShiftName AS ShiftName
                                      , s.ShiftStartDateTime AS ShiftStartDatetime
                                      , s.ShiftEndDatetime AS ShiftEndDatetime
                                      , e.updated_time AS TransactionTime
                                      , e.id AS Id
                                      , e.entity_id AS ContainerNbr
                                      , e.entity_type AS ContainerType
                                      , e.entity_status AS ContainerStatus
                                      , e.created_time AS CreatedTime
                                      , ROW_NUMBER() OVER(ORDER BY e.id, e.entity_status) AS RowNbr
                              FROM    `mtech-dc2-prod.inventory.entity` e
                                      INNER JOIN shifts s
                                          ON e.updated_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime
                              WHERE   e.entity_type IN ('TOTE', 'CRT')
                                      AND e.entity_status IN ('SIP', 'IPK')
                                      AND e.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = e.entity_status AND updated_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime)

                              UNION ALL

                              SELECT    s.ShiftDate AS ShiftDate
                                      , s.ShiftName AS ShiftName
                                      , s.ShiftStartDateTime AS ShiftStartDatetime
                                      , s.ShiftEndDatetime AS ShiftEndDatetime
                                      , e.updated_time AS TransactionTime
                                      , e.id AS Id
                                      , e.entity_id AS ContainerNbr
                                      , e.entity_type AS ContainerType
                                      , e.entity_status AS ContainerStatus
                                      , e.created_time AS CreatedTime
                                      , ROW_NUMBER() OVER(ORDER BY e.id, e.entity_status) AS RowNbr
                              FROM    `mtech-dc2-prod.inventory.entity` e
                                      INNER JOIN shifts s
                                          ON e.created_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime
                              WHERE   e.entity_type = 'TOTE'
                                      AND e.entity_status ='CRE'
                                      AND e.action = 'ADD'

                             UNION ALL

                             SELECT   s.ShiftDate AS ShiftDate
                                      , s.ShiftName AS ShiftName
                                      , s.ShiftStartDateTime AS ShiftStartDatetime
                                      , s.ShiftEndDatetime AS ShiftEndDatetime
                                      , e.updated_time AS TransactionTIme
                                      , e.id AS Id
                                      , e.entity_id AS ContainerNbr
                                      , e.entity_type AS ContainerType
                                      , e.entity_status AS ContainerStatus
                                      , e.created_time AS CreatedTime
                                      , ROW_NUMBER() OVER(ORDER BY e.id, e.entity_status) AS RowNbr
                              FROM    `mtech-dc2-prod.inventory.entity` e
                                      INNER JOIN shifts s
                                          ON e.updated_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime
                              WHERE   e.entity_type IN ('CSE', 'BINBOX')
                                      AND e.entity_status IN ('CRE', 'PIK', 'PTW')
                                      AND e.version_id = (SELECT MIN(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = e.entity_status AND updated_time BETWEEN s.ShiftStartDatetime AND s.ShiftEndDatetime)
                                      AND NOT EXISTS (SELECT 1 FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = e.entity_status AND updated_time < s.ShiftStartDatetime)
                              )

SELECT      rpt.ShiftDate
            , rpt.ShiftName
            , CASE WHEN rpt.LgclLocnNbr = 7254 THEN 'HAF' WHEN rpt.WaveNbr IS NOT NULL THEN 'BKG' ELSE pa.ProcessArea END AS ProcessArea
            , IFNULL(rpt.WaveNbr, CAST(po.PoNbr AS STRING)) AS PoNbr
            , IFNULL(rpt.WaveNbr, rpt.RcptNbr) AS RcptNbr
            , IF(rpt.WaveNbr IS NULL, 0, 1) AS WaveFlag
            , CASE WHEN rpt.WaveNbr IS NOT NULL THEN WV.WaveQty ELSE po.PoQty END AS OrderedQty
            , SUM(RcvdQty) AS RcvdQty
            , SUM(TktdQty) AS TktdQty
            , SUM(PrpdQty) AS PrpdQty
            , SUM(PtlQty)  AS PtlQty
            , SUM(PtwyQty) AS PtwyQty
            , SUM(PckdQty) AS PckdQty
            , SUM(PsrtQty) AS PsrtQty
            , SUM(ShpdQty) AS ShpdQty
            , CASE WHEN shifts.shiftdate IS NOT NULL THEN 1 ELSE 0 END AS IsDaily
            , CASE WHEN shifts.shiftdate IS NOT NULL AND rpt.shiftname = 'SHIFT1' THEN 1 ELSE 0 END AS IsMidDay
FROM        (
            SELECT    cd.ShiftDate AS ShiftDate
                      , cd.ShiftName AS ShiftName
                      , cd.ShiftStartDatetime
                      , cd.ShiftEndDatetime
                      , IF(cd.ContainerStatus = 'PIK', NULL, xref.RcptNbr) AS RcptNbr
                      , IF(cd.ContainerStatus = 'PIK', xref.WaveNbr, NULL) AS WaveNbr
                      , ss.id
                      , cd.ContainerNbr AS ContainerNbr
                      , cd.ContainerStatus
                      , ss.item AS ItemNbr
                      , ss.lgcl_locn_nbr AS LgclLocnNbr
                      , CASE cd.ContainerStatus WHEN 'CRE' THEN ss.quantity + IFNULL(adj.AdjdQty, 0) ELSE 0 END AS RcvdQty
                      , CASE cd.ContainerStatus WHEN 'PRT' THEN ss.quantity + IFNULL(adj.AdjdQty, 0) ELSE 0 END AS TktdQty
                      , CASE WHEN cd.ContainerType = 'TOTE' AND cd.ContainerStatus = 'VSC' THEN ss.quantity + IFNULL(adj.AdjdQty, 0) ELSE 0 END AS PrpdQty
                      , 0 AS PtlQty
                      , CASE cd.ContainerStatus WHEN 'PTW' THEN ss.quantity + IFNULL(adj.AdjdQty, 0) ELSE 0 END AS PtwyQty
                      , CASE cd.ContainerStatus WHEN 'PIK' THEN ss.quantity + IFNULL(adj.AdjdQty, 0) ELSE 0 END AS PckdQty
                      , 0 AS PsrtQty
                      , 0 AS ShpdQty
            FROM      container_derived cd
                      INNER JOIN snapshot_entity_xref xref
                          ON xref.EntityContainerId = cd.Id
                      INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                          ON ss.id = xref.SnapshotId
                      LEFT JOIN (
                                SELECT    ss1.id
                                          , ia.created_time
                                          , SUM(ia.current_quantity - ia.previous_quantity) AS AdjdQty
                                FROM      `mtech-dc2-prod.inventory.inventory_snapshot` ss1
                                          INNER JOIN `mtech-dc2-prod.inventory.inventory_adjustment_history` ia
                                              ON ia.container = ss1.container
                                              AND ia.item = ss1.item
                                WHERE     ss1.version_id = 0
                                          AND ss1.id = (SELECT MAX(id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE container = ss1.container AND item = ss1.item AND created_time <= ia.created_time)
                                GROUP BY  ss1.id
                                          , ia.created_time
                                ) AS adj
                          ON adj.id = ss.id
                          AND adj.created_time > ss.updated_time
            WHERE     cd.ContainerType IN ('TOTE','BINBOX', 'CSE')
                      AND cd.ContainerStatus IN ('CRE', 'PRT', 'VSC','PTW', 'PIK')
                      AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id AND updated_time <= cd.TransactionTime)

            UNION ALL

            SELECT    cd.ShiftDate AS ShiftDate
                      , cd.ShiftName AS ShiftName
                      , cd.ShiftStartDatetime
                      , cd.ShiftEndDatetime
                      , xref.RcptNbr AS RcptNbr
                      , xref.WaveNbr AS WaveNbr
                      , ss.id
                      , cd.ContainerNbr AS ContainerNbr
                      , cd.ContainerStatus
                      , ss.item AS ItemNbr
                      , ss.lgcl_locn_nbr AS LgclLocnNbr
                      , 0 AS RcvdQty
                      , 0 AS TktdQty
                      , 0 AS PrpdQty
                      , CASE cd.ContainerStatus WHEN 'IPK' THEN ss.quantity - IFNULL(p.quantity, 0) ELSE 0 END AS PtlQty
                      , 0 AS PtwyQty
                      , 0 AS PckdQty
                      , CASE cd.ContainerStatus WHEN 'SIP' THEN ss.quantity - IFNULL(p.quantity, 0) ELSE 0 END as PsrtQty
                      , CASE WHEN cd.ContainerStatus IN ('MFT', 'SHP') THEN ss.quantity ELSE 0 END AS ShpdQty
              FROM    container_derived cd
                      INNER JOIN snapshot_entity_xref xref
                          ON xref.EntityContainerId = cd.Id
                      INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                          ON ss.id = xref.SnapshotId
                      LEFT JOIN (
                                SELECT  cd1.ShiftDate
                                        , cd1.ShiftName
                                        , cd1.ContainerStatus
                                        , ss1.id
                                        , ss1.quantity
                                FROM    `mtech-dc2-prod.inventory.inventory_snapshot` ss1
                                        INNER JOIN container_derived cd1
                                            ON cd1.ContainerNbr = ss1.container
                                            AND cd1.ContainerType IN ('TOTE', 'CRT')
                                WHERE   ss1.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss1.id AND updated_time < cd1.ShiftStartDatetime)
                                ) AS p
                          ON p.id = ss.id
                          AND p.ShiftDate = cd.ShiftDate
                          AND p.ShiftName = cd.ShiftName
                          AND p.ContainerStatus = cd.ContainerStatus
              WHERE   cd.ContainerType IN ('TOTE', 'CRT')
                      AND cd.ContainerStatus IN ('SIP', 'IPK', 'MFT', 'SHP')
                      AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id AND updated_time <= TIMESTAMP_ADD(cd.TransactionTime, INTERVAL 2 SECOND))
            ) AS rpt
            LEFT JOIN (
                      SELECT  distinct CAST(r.rcpt_nbr AS STRING) AS RcptNbr
                              , r.po_nbr AS PoNbr
                              , h.tot_po_ordr_qty AS PoQty
                      FROM    `mtech-dc2-prod.orders.order_rcpt` r
                              INNER JOIN `mtech-dc2-prod.orders.po_hdr` h
                                  ON h.po_nbr = r.po_nbr
                      WHERE   r.last_modified_ts = (SELECT MAX(last_modified_ts) FROM `mtech-dc2-prod.orders.order_rcpt` WHERE rcpt_nbr = r.rcpt_nbr)
                              AND h.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.orders.po_hdr` WHERE po_nbr = h.po_nbr)
                      ) AS po
                ON po.RcptNbr = rpt.RcptNbr
            LEFT JOIN (
                      SELECT  distinct h.po_nbr AS PoNbr
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
                      ) AS pa
                ON pa.PONbr = po.PONbr
            LEFT JOIN (
                      SELECT  CAST(w.wave_nbr AS STRING) WaveNbr
                              , w.total_qty WaveQty
                      FROM    `mtech-dc2-prod.waving.wave` w
                      WHERE   w.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.waving.wave` WHERE wave_nbr = w.wave_nbr)
                      ) AS wv
                ON wv.WaveNbr = rpt.WaveNbr
            LEFT JOIN SHIFTS
                ON shifts.shiftdate = rpt.shiftdate
                AND current_timestamp() BETWEEN shifts.ShiftStartDatetime AND shifts.ShiftEndDatetime
GROUP BY    rpt.ShiftDate
            , rpt.ShiftName
            , rpt.lgcllocnnbr
            , ProcessArea
            , PoNbr
            , RcptNbr
            , WaveFlag
            , OrderedQty
            , shifts.shiftdate
HAVING      RcvdQty > 0 OR TktdQty > 0 OR PrpdQty > 0 OR PtlQty > 0 OR PtwyQty > 0 OR PckdQty > 0 OR PsrtQty > 0 OR ShpdQty > 0
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: shift_date {
    type: date
#     sql: ${TABLE}.ShiftDate ;;
    sql: cast(${TABLE}.ShiftDate as timestamp) ;;

  }

  dimension: Shift_date_formatted {
    label: "Date >> Shift >> Process PO / Wave"
    sql: ${shift_date} ;;
    html: {{ rendered_value | date: "%a,%b %m, %Y" }} ;;
  }

#   dimension_group: shift_date_timestamp {
#     type: time
#     sql: ${shift_date} ;;
#   }
#   dimension_group: ShiftStartDatetime {
#     type: time
#     sql: ${TABLE}.ShiftStartDatetime ;;
#   }
#
#   dimension_group: ShiftEndDatetime {
#     type: time
#     sql: ${TABLE}.ShiftEndDatetime ;;
#   }
#
# # Date Range logic start
#
#   filter: date_filter {
#     type: date
#     description: "To be used with dimension date filter"
#   }
#
#   dimension: is_date_filter_date {
#     type: yesno
#     sql: cast(${ShiftStartDatetime_date} as date) >= {% date_start date_filter %}
#         AND cast(${ShiftEndDatetime_date} as date) < {% date_end date_filter %} ;;
#   }
#
#   dimension_group: in_date_filter {
#     type: duration
#     sql_start: {% date_start date_filter %} ;;
#     sql_end:   {% date_end date_filter %} ;;
#   }
#
#   dimension_group: in_shift_date_range {
#     type: duration
#     sql_start: {% date_start ShiftStartDatetime_date %} ;;
#     sql_end:   {% date_end   ShiftEndDatetime_date %} ;;
#   }
#
# # Date Range logic end

  dimension: shift_name {
    label: "Shift"
    type: string
    sql: ${TABLE}.ShiftName ;;
  }

  dimension: process_area {
    type: string
    hidden: yes
    sql: ${TABLE}.ProcessArea ;;
  }

  dimension: process_area_full {
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
    label: "Received"
    primary_key: yes
    type: string
    sql: ${TABLE}.RcptNbr ;;
  }

  dimension: wave_flag {
    type: number
    sql: ${TABLE}.WaveFlag ;;
  }

  measure: ordered_qty {
    label: "Ordered"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.OrderedQty ;;
  }

  measure: rcvd_qty {
    label: "Received"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.RcvdQty ;;
  }

  measure: tktd_qty {
    label: "Ticketed"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.TktdQty ;;
  }

  measure: prpd_qty {
    label: "Prepped"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.PrpdQty ;;
  }

  measure: ptl_qty {
    label: "PTL"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.PtlQty ;;
  }

  measure: ptwy_qty {
    label: "Putaway"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.PtwyQty ;;
  }

  measure: pckd_qty {
    label: "Picked"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.PckdQty ;;
  }

  measure: psrt_qty {
    label: "PreSort"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.PsrtQty ;;
  }

  measure: shpd_qty {
    label: "Shipped"
    type: sum
    value_format:"#,##0"
    sql: ${TABLE}.ShpdQty ;;
  }

  dimension: is_daily {
    type: number
    sql: ${TABLE}.IsDaily ;;
  }

  dimension: is_mid_day {
    type: number
    sql: ${TABLE}.IsMidDay ;;
  }
  dimension: macys_logo {
    type: string
    sql: ${wave_flag}
    html: <img src="https://content-az.equisolve.net/_724c7f58341cc8e9580e487fa7ca4cbb/macysinc/db/414/5629/image_thumbnail.png" /> ;;
  }

  set: detail {
    fields: [
      shift_date,
      shift_name,
      process_area,
      po_nbr,
      rcpt_nbr,
      wave_flag,
      is_daily,
      is_mid_day
    ]
  }
}
