view: bda_shipped_summary {
  derived_table: {
    datagroup_trigger: macys_datagroup
    partition_keys: ["ShiftDate"]
    cluster_keys: ["Division"]

    sql: WITH  shiftTimings AS (
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

SELECT    CASE ss.lgcl_locn_nbr WHEN 7254 THEN 'HAF' ELSE 'BKG' END AS Division
          , shifts.ShiftDate
          , shifts.ShiftName
          , COUNT(DISTINCT cea.attribute_value) AS ShippedPOCount
          , COUNT(DISTINCT ec.entity_id) AS ShippedCartonCount
          , SUM(ss.quantity) AS ShippedUnitCount
FROM      `mtech-dc2-prod.inventory.entity` ec
          INNER JOIN shifts
              ON ec.updated_time BETWEEN shifts.ShiftStartDatetime AND shifts.ShiftEndDatetime
          INNER JOIN `mtech-dc2-prod.inventory.inventory_snapshot` ss
              ON ss.container = ec.entity_id
          INNER JOIN `mtech-dc2-prod.inventory.entity` ei
              ON ei.entity_id = CAST(ss.id AS STRING)
              AND ei.version_id = 0
          INNER JOIN `mtech-dc2-prod.inventory.common_entity_attributes` cea
              ON cea.entity_id = ei.id
              AND cea.enabled = 1
              AND cea.attribute_name IN ('PO', 'WaveNumber')
WHERE     ec.entity_type = 'CRT'
          AND ec.entity_status IN ('MFT', 'SHP')
          AND ec.version_id = (SELECT MIN(version_id) FROM `mtech-dc2-prod.inventory.entity` WHERE id = ec.id AND entity_status IN ('MFT', 'SHP'))
          AND ss.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.inventory_snapshot` WHERE id = ss.id AND updated_time <= ec.updated_time)
          AND cea.version_id = (SELECT MAX(version_id) FROM `mtech-dc2-prod.inventory.common_entity_attributes` WHERE entity_id = cea.entity_id AND attribute_name = cea.attribute_name)
GROUP BY  Division
          , ShiftDate
          , ShiftName
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: division {
    type: string
    primary_key: yes
    sql: ${TABLE}.Division ;;
  }

  dimension: shift_date {
    type: date
    sql: ${TABLE}.ShiftDate ;;
  }
  dimension: Shift_date_formatted {
    label: "Date/Shift"
    sql: ${shift_date} ;;
    html: {{ rendered_value | date: "%a,%b %m, %Y" }} ;;
  }

  dimension: shift_name {
    type: string
    sql: ${TABLE}.ShiftName ;;
  }

  dimension: shipped_pocount {
    type: number
    sql: ${TABLE}.ShippedPOCount ;;
  }

  dimension: shipped_carton_count {
    type: number
    sql: ${TABLE}.ShippedCartonCount ;;
  }

  dimension: shipped_unit_count {
    type: number
    sql: ${TABLE}.ShippedUnitCount ;;
  }



  set: detail {
    fields: [
      division,
      shift_date,
      shift_name
    ]
  }
}
