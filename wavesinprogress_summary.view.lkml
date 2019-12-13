view: wavesinprogress_summary {
  derived_table: {
    sql: WITH    entity_container AS (
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
                                            , e.Id AS EntityInventoryId
                                            , ec.EntityId AS EntityContainerId
                                            , ceai.attribute_value AS WaveNbr
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
                                            INNER JOIN    (
                                                          SELECT    entity_id
                                                                    , attribute_name
                                                                    , attribute_value
                                                                    , RANK() OVER (PARTITION BY entity_id ORDER BY id DESC, version_id DESC) AS RowNbr
                                                          FROM      `mtech-dc2-prod.inventory.common_entity_attributes`
                                                          WHERE     attribute_name = 'WaveNumber'
                                                          ) AS ceai
                                                ON ceai.entity_id = e.id
                                                AND ceai.RowNbr = 1
                                  WHERE     ss.version_id = 0
                                  )

      , wave_summary AS   (
                          SELECT    wlc.wave_id
                                    , CAST(w.wave_nbr AS STRING) AS WaveNumber
                                    , CASE wlc.status
                                          WHEN 'REQ' THEN 'Requested'
                                          WHEN 'ALC' THEN 'Allocated'
                                          WHEN 'RLS' THEN 'Released'
                                          WHEN 'PIKIP' THEN 'Pick in Progress'
                                          WHEN 'SPK' THEN 'Staged'
                                          WHEN 'PSR' THEN 'PreSort Ready'
                                          WHEN 'PSIP' THEN 'PreSort in Progress'
                                          WHEN 'PTSR' THEN 'Put to Store Ready'
                                          WHEN 'IPK' THEN 'Pack in Progress'
                                          WHEN 'PTSIP' THEN 'Put to Store in Progress'
                                          WHEN 'PIP' THEN 'Pack in Progress'
                                          WHEN 'SHPIP' THEN 'Ship in Progress'
                                          WHEN 'SHPD' THEN 'Ship Complete'
                                          ELSE wlc.status
                                    END AS WaveStatus
                                    , w.total_qty
                                    , w.order_count
                                    , w.flow_Type
                                    , CASE w.flow_type
                                          WHEN 'HAF' THEN 'Hold & Flow'
                                          WHEN 'PMR' THEN 'Poolstock Movement Request'
                                          ELSE w.flow_Type
                                      END AS FlowTypeDesc
                                    , w.ship_out_start_dt
                                    , w.ship_out_end_dt
                                    , ROW_NUMBER() OVER (PARTITION BY wlc.wave_id ORDER BY wlc.version_id DESC, wlc.updated_time DESC) RowNbr
                                    , wc.wave_type
                          FROM      `mtech-dc2-prod.waving.wave_life_cycle` wlc
                                    INNER JOIN `mtech-dc2-prod.waving.wave` w
                                        ON w.id = wlc.wave_id
                                    INNER JOIN `mtech-dc2-prod.waving.wave_config` wc
                                        ON wc.id = wlc.wave_id
                          WHERE     wlc.id= (SELECT MAX(id) FROM `mtech-dc2-prod.waving.wave_life_cycle` WHERE wave_id = wlc.wave_id)
                                    AND w.updated_time = (SELECT MAX(updated_time) FROM `mtech-dc2-prod.waving.wave` WHERE id = w.id)
                          )

      , order_summary AS  (
                          SELECT    li.wave_id
                                    , MIN(expected_ship_date) SOTDate
                                    , COUNT(DISTINCT sh.ship_to_loc_nbr) Stores
                                    , COUNT(DISTINCT CASE CAST(sh.expected_ship_date AS DATE) WHEN CURRENT_DATE() THEN sh.order_shipment_nbr ELSE NULL END) AS SOTShipments
                                    , SUM(CASE CAST(sh.expected_ship_date AS DATE) WHEN CURRENT_DATE() THEN li.alloc_qnty ELSE 0 END) AS SOTUnits
                                    , COUNT(DISTINCT sh.order_shipment_nbr) Shipments
                                    , SUM(li.alloc_qnty) Units
                          FROM      `mtech-dc2-prod.orderfulfillment.shipment` sh
                                    INNER JOIN `mtech-dc2-prod.orderfulfillment.lineitem_wave_event` li
                                        ON li.order_ship_nbr = sh.order_shipment_nbr
                                    INNER JOIN wave_summary ws
                                        ON ws.wave_id = li.wave_id
                          WHERE     sh.updated_ts = (SELECT MAX(sh.updated_ts) FROM `mtech-dc2-prod.orderfulfillment.shipment` WHERE order_id = sh.order_id)
                                    AND li.updated_ts = (SELECT MAX(li.updated_ts) FROM `mtech-dc2-prod.orderfulfillment.lineitem_wave_event` WHERE order_ship_nbr = li.order_ship_nbr)
                          GROUP BY  li.wave_id
                          )

      , wave_activity AS  (
                          SELECT    wlc.wave_id
                                    , MIN(CASE wlc.status WHEN 'REQ' THEN wlc.updated_time ELSE NULL END) AS Requested
                                    , MIN(CASE wlc.status WHEN 'ALC' THEN wlc.updated_time ELSE NULL END) AS Allocated
                                    , MIN(CASE wlc.status WHEN 'RLS' THEN wlc.updated_time ELSE NULL END) AS Released
                                    , MIN(CASE wlc.status WHEN 'PIKIP' THEN wlc.updated_time ELSE NULL END) AS PickInProgress
                                    , MIN(CASE wlc.status WHEN 'SPK' THEN wlc.updated_time ELSE NULL END) AS Staged
                                    , MIN(CASE wlc.status WHEN 'PSR' THEN wlc.updated_time ELSE NULL END) AS PreSortReady
                                    , MIN(CASE wlc.status WHEN 'PSIP' THEN wlc.updated_time ELSE NULL END) AS PreSortInProgress
                                    , MIN(CASE wlc.status WHEN 'PTSR' THEN wlc.updated_time ELSE NULL END) AS PutToStoreReady
                                    , MIN(CASE wlc.status WHEN 'PTSIP' THEN wlc.updated_time ELSE NULL END) AS PutToStoreInProgress
                                    , MIN(CASE wlc.status WHEN 'SHPIP' THEN wlc.updated_time ELSE NULL END) AS ShipInProgress
                                    , MIN(CASE wlc.status WHEN 'SHPD' THEN wlc.updated_time ELSE NULL END) AS ShipComplete
                                    , MIN(CASE wlc.status WHEN 'PIP' THEN wlc.updated_time ELSE NULL END) AS PackInProgress
                          FROM      `mtech-dc2-prod.waving.wave_life_cycle` wlc
                                    INNER JOIN wave_summary ws
                                        ON ws.wave_id = wlc.wave_id
                          GROUP BY  wlc.wave_id
                          )

      , pick_activity AS   (
                          SELECT    x.WaveNbr AS WaveNumber
                                    , COUNT(DISTINCT a.container) AS Cases
                                    , COUNT(DISTINCT CASE e.entity_status WHEN 'PIK' THEN a.container ELSE NULL END) AS PickedCases
                                    , SUM(CASE e.entity_Status WHEN 'PIK' THEN ss.quantity ELSE 0 END) AS PickedUnits
                                    , MIN(CASE e.entity_status WHEN 'PIK' THEN e.updated_time ELSE NULL END) AS PickStart
                                    , MAX(CASE e.entity_status WHEN 'PIK' THEN e.updated_time ELSE NULL END) AS PickEnd
                                    , COUNT(DISTINCT CASE e.entity_status WHEN 'SPK' THEN a.container ELSE NULL END) AS StagedCases
                                    , SUM(CASE e.entity_status WHEN 'SPK' THEN ss.quantity ELSE 0 END) AS StagedUnits
                                    , MIN(CASE e.entity_status WHEN 'SPK' THEN e.updated_time ELSE NULL END) AS StageStart
                                    , MAX(CASE e.entity_status WHEN 'SPK' THEN e.updated_time ELSE NULL END) AS StageEnd
                          FROM      `mtech-dc2-prod.wsm.activity` a
                                    INNER JOIN snapshot_entity_xref x
                                        ON x.container = a.container
                                    INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                        ON e.id = x.EntityContainerId
                                    INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                        ON ss.id = x.SnapshotId
                          WHERE     a.type in ('CASEPULL','BINPULL')
                                    AND a.version = (SELECT MAX(version) FROM `mtech-dc2-prod.wsm.activity` WHERE id = a.id)
                                    AND e.version_Id = (SELECT MIN(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_status = e.entity_Status)
                                    AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id AND updated_time <= TIMESTAMP_ADD(e.updated_time, INTERVAL 2 SECOND))
                          GROUP BY  WaveNumber
                          )

      , presort_activity AS   (
                              SELECT    x.WaveNbr AS WaveNumber
                                        , MIN(e.created_time) AS PreSortStart
                                        , MAX(e.updated_Time) AS PreSortEnd
                                        , COUNT(DISTINCT e.entity_id) AS Totes
                                        , SUM(ss.quantity) AS Units
                              FROM      snapshot_entity_xref x
                                        INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                            ON e.id = x.EntityContainerId
                                            AND e.entity_status = 'SIP'
                                        INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                            ON ss.id = x.SnapshotId
                              WHERE     x.container_type_id = 4
                                        AND e.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id AND entity_Status = e.entity_status)
                                        AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id AND updated_time <= TIMESTAMP_ADD(e.updated_time,  INTERVAL 2 SECOND))
                              GROUP BY  WaveNumber
                              )

      , put_activity AS     (
                            SELECT    x.WaveNbr AS WaveNumber
                                      , MIN(e.created_time) AS PutStart
                                      , MAX(e.updated_time) AS PutEnd
                                      , COUNT(DISTINCT e.entity_id) AS Cartons
                                      , SUM(ss.quantity) AS Units
                            FROM      snapshot_entity_xref x
                                      INNER JOIN `mtech-dc2-prod.inventory.entity` e
                                          ON e.id = x.EntityInventoryId
                                      INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
                                          ON ss.id = x.SnapshotId
                                          AND ss.enabled = 1
                            WHERE     x.container_type_id = 1
                                      AND e.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = e.id)
                                      AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id)
                            GROUP BY  WaveNumber
                            )

SELECT    w.WaveNumber AS WaveNumber
          , w.WaveStatus AS Status
          , w.total_qty AS TotalQty
          , w.order_count AS OrderCount
          , w.flow_type AS FlowType
          , w.wave_type AS WaveType
          , w.FlowTypeDesc
          , DATETIME(wa.Requested, "America/New_York") as Requested
          , DATETIME(wa.Allocated, "America/New_York") as Allocated
          , DATETIME(wa.Released, "America/New_York") as Released
          , DATETIME(wa.PickInProgress, "America/New_York") as PickInProgress
          , DATETIME(wa.Staged, "America/New_York") as Staged
          , DATETIME(wa.PreSortReady, "America/New_York") as PreSortReady
          , DATETIME(wa.PreSortInProgress, "America/New_York") as PreSortInProgress
          , DATETIME(wa.PutToStoreReady, "America/New_York") as PutToStoreReady
          , DATETIME(wa.PutToStoreInProgress, "America/New_York") as PutToStoreInProgress
          , DATETIME(wa.ShipInProgress, "America/New_York") as ShipInProgress
          , DATETIME(wa.ShipComplete, "America/New_York") as ShipComplete
          , DATETIME(wa.PackInProgress, "America/New_York") as PackInProgress
          , p.Cases
          , p.PickedCases AS CasesPicked
          , p.StagedCases AS CasesStaged
          , p.PickedUnits
          , p.StagedUnits
          , ps.Totes
          , ps.Units AS PreSortUnits
          , pa.Cartons
          , pa.units AS PutUnits
          , o.SOTDate
          , o.Stores
          , o.SOTShipments
          , o.SOTUnits
          , o.shipments AS Shipments
          , o.units AS AllocatedUnits
FROM      wave_summary w
          INNER JOIN order_summary o
              ON o.wave_id = w.wave_id
          LEFT JOIN wave_activity wa
              ON wa.wave_id = w.wave_id
          LEFT JOIN pick_activity p
              ON p.WaveNumber = w.WaveNumber
          LEFT JOIN presort_activity ps
              ON ps.WaveNumber = w.WaveNumber
          LEFT JOIN put_activity pa
              ON pa.WaveNumber = w.WaveNumber
where w.WaveStatus not in ('CXL','CLS','FAIL')
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: wave_number {
    primary_key: yes
    type: string
    label: "Wave"
    sql: ${TABLE}.WaveNumber ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.Status ;;
  }

  measure: total_qty {
    type: sum
    sql: ${TABLE}.TotalQty ;;
  }

  measure: order_count {
    type: sum
    sql: ${TABLE}.OrderCount ;;
  }

  dimension: flow_type {
    type: string
    sql: ${TABLE}.FlowType ;;
  }

  dimension: wave_type {
    label: "Wave Group"
    type: string
    sql: ${TABLE}.WaveType ;;
  }

  dimension: flow_type_desc {
    type: string
    sql: ${TABLE}.FlowTypeDesc ;;
  }

  dimension_group: requested {
    type: time
    sql: ${TABLE}.Requested ;;
  }

  dimension_group: allocated {
    type: time
    sql: ${TABLE}.Allocated ;;
  }

  dimension_group: released {
    type: time
    sql: ${TABLE}.Released ;;
  }

  dimension_group: pick_in_progress {
    type: time
    sql: ${TABLE}.PickInProgress ;;
  }

  dimension_group: staged {
    type: time
    sql: ${TABLE}.Staged ;;
  }

  dimension_group: pre_sort_ready {
    type: time
    sql: ${TABLE}.PreSortReady ;;
  }

  dimension_group: pre_sort_in_progress {
    type: time
    sql: ${TABLE}.PreSortInProgress ;;
  }

  dimension_group: put_to_store_ready {
    type: time
    sql: ${TABLE}.PutToStoreReady ;;
  }

  dimension_group: put_to_store_in_progress {
    type: time
    sql: ${TABLE}.PutToStoreInProgress ;;
  }

  dimension_group: ship_in_progress {
    type: time
    sql: ${TABLE}.ShipInProgress ;;
  }

  dimension_group: ship_complete {
    type: time
    sql: ${TABLE}.ShipComplete ;;
  }

  dimension_group: pack_in_progress {
    type: time
    sql: ${TABLE}.PackInProgress ;;
  }

  measure: cases {
    type: sum
    sql: ${TABLE}.Cases ;;
  }

  measure: cases_picked {
    label: "Picked"
    type: sum
    sql: ${TABLE}.CasesPicked ;;
  }

  measure: cases_staged {
    type: sum
    sql: ${TABLE}.CasesStaged ;;
  }

  measure: picked_units {
    type: sum
    sql: ${TABLE}.PickedUnits ;;
  }

  measure: staged_units {
    type: sum
    sql: ${TABLE}.StagedUnits ;;
  }

  measure: totes {
    type: sum
    sql: ${TABLE}.Totes ;;
  }

  measure: pre_sort_units {
    type: sum
    sql: ${TABLE}.PreSortUnits ;;
  }

  measure: cartons {
    type: sum
    sql: ${TABLE}.Cartons ;;
  }

  measure: put_units {
    type: sum
    sql: ${TABLE}.PutUnits ;;
  }

  dimension: sotdate {
    label: "SOT Date"
    type: string
    sql: ${TABLE}.SOTDate ;;
  }

  measure: stores {
    type: sum
    sql: ${TABLE}.Stores ;;
  }

  measure: sotshipments {
    type: sum
    sql: ${TABLE}.SOTShipments ;;
  }

  measure: sotunits {
    type: sum
    sql: ${TABLE}.SOTUnits ;;
  }

  measure: shipments {
    type: sum
    sql: ${TABLE}.Shipments ;;
  }

  measure: allocated_units {
    type: sum
    sql: ${TABLE}.AllocatedUnits ;;
  }

#   measure: Pick_percent {
#     type: sum
#     sql: sum(${cases_picked})/sum(${cases}) ;;
#   }
#
#   measure: PreSort_percent {
#     type: sum
#     sql: ${pre_sort_units}/${total_qty} ;;
#   }
#
#   measure: Put_percent {
#     type: sum
#     sql: ${put_units}/${total_qty} ;;
#   }
#
#   measure: Stage_percent {
#     type: sum
#     sql: ${cases_staged}/${cases} ;;
#   }

  set: detail {
    fields: [
      wave_number,
      status,
      total_qty,
      order_count,
      flow_type,
      wave_type,
      flow_type_desc,
      requested_time,
      allocated_time,
      released_time,
      pick_in_progress_time,
      staged_time,
      pre_sort_ready_time,
      pre_sort_in_progress_time,
      put_to_store_ready_time,
      put_to_store_in_progress_time,
      ship_in_progress_time,
      ship_complete_time,
      pack_in_progress_time,
      cases,
      cases_picked,
      cases_staged,
      picked_units,
      staged_units,
      totes,
      pre_sort_units,
      cartons,
      put_units,
      sotdate,
      stores,
      sotshipments,
      sotunits,
      shipments,
      allocated_units
    ]
  }
}
