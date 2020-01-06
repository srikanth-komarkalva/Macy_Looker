include: "BDA_Reports.model.lkml"

view: containers {
  derived_table: {
#     datagroup_trigger: macys_datagroup
#     partition_keys: ["updatedTime"]
#     cluster_keys: ["waveNumber","container","status"]

    sql: select
        Distinct
        cea.attribute_value waveNumber
        , e.entity_id container
        , e.entity_Type containerType
        , ss.item upc
        , ss.quantity units
        , e.updated_time updatedTime
        , e.entity_status status
        , cr.parent location
      from
       `mtech-dc2-prod.inventory.entity` e
        inner join `mtech-dc2-prod.inventory.common_entity_attributes` cea
          on cea.entity_id = e.id
          and cea.attribute_name = 'WaveNumber'
          and cea.enabled = 1
        inner join `mtech-dc2-prod.inventory.inventory_snapshot` ss
          on ss.container = e.entity_id
          and ss.enabled = 1
        Inner join `mtech-dc2-prod.inventory.container_relationship` cr on cr.child = e.entity_id
      where
        (
        (
        e.entity_Type = 'TOTE'
        and coalesce(cr.parent,null) between 'HF10B031' and 'HF15A048'
        and e.entity_status not in ('PTS')
        )
        or
        (
        e.entity_type = 'CSE'
        )

        )
        and e.enabled = 1
        and e.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.entity` where id = e.id)
        and ss.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.inventory_snapshot` where ss.id = id)
        and cea.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.common_entity_attributes` where id = cea.id)
          and cr.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.container_relationship` where id = cr.id)
        and cr.enabled = 1

      Order by 1,2,4

       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: wave_number {
    type: string
    primary_key: yes
    sql: ${TABLE}.waveNumber ;;
  }

  dimension: container {
    type: string
    sql: ${TABLE}.container ;;
  }

  dimension: macys_logo {
    type: string
    sql: ${container_type};;
    html: <img src="https://content-az.equisolve.net/_724c7f58341cc8e9580e487fa7ca4cbb/macysinc/db/414/5629/image_thumbnail.png" /> ;;
  }

  dimension: container_type {
    type: string
    sql: ${TABLE}.containerType ;;
  }

  dimension: upc {
    type: string
    sql: ${TABLE}.upc ;;
  }

  dimension: units {
    type: number
    sql: ${TABLE}.units ;;
  }

  dimension_group: updated_time {
    type: time
    html: {{ rendered_value | date: "%m/%d/%y %H:%M %p" }} ;;
    sql: ${TABLE}.updatedTime ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: location {
    type: string
    sql: ${TABLE}.location ;;
  }

  measure: count_of_UPC {
    type: count_distinct
    sql: ${upc} ;;
  }

  measure: Sum_of_units {
    type: sum
    sql: ${units} ;;
  }
  set: detail {
    fields: [
      wave_number,
      container,
      container_type,
      upc,
      units,
      updated_time_time,
      status,
      location
    ]
  }
}
