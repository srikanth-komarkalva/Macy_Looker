include: "PowerBi_Reports.model.lkml"

view: casestopick {
  derived_table: {
    datagroup_trigger: macys_datagroup
    sql: Select
          cea.attribute_value WaveNumber
          , a.id TaskNumber
          , sts.status_cd TaskStatus
          , e.entity_id CaseNumber
          , e.entity_status CaseStatus
          , DATETIME(alc.updated_time, "America/New_York") as LastUpdated
          , alc.updated_by UpdatedByUser
          , cr.parent location
          , substr(cr.parent,1,4) Aisle
          , substr(cr.parent,1,2) AreaZone
        from
          `mtech-dc2-prod.wsm.activity` a
          inner join `mtech-dc2-prod.wsm.activity_life_cycle` alc on alc.activity_id = a.id
          inner join `mtech-dc2-prod.wsm.activity_status` sts on sts.id = alc.status_id
          inner join `mtech-dc2-prod.inventory.entity` e on e.entity_id = a.container
          inner join `mtech-dc2-prod.inventory.common_entity_attributes` cea
            on cea.entity_id = e.id
            and cea.attribute_name = 'WaveNumber'
          inner join `mtech-dc2-prod.inventory.container_relationship` cr on cr.child = a.container and cr.enabled = 1
        where
          e.entity_type in ('CSE','BINBOX')
          and e.entity_status = 'RSV'
          and e.enabled = 1
          and a.enabled = 1
          and alc.enabled = 1
          and cea.enabled = 1
          and alc.status_id not in (3,4,8)
          and e.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.entity` where id = e.id)
          and cea.version_Id = (select max(version_id) from `mtech-dc2-prod.inventory.common_entity_attributes` where id = cea.id)
          and alc.version = (select max(version) from `mtech-dc2-prod.wsm.activity_life_cycle` where id = alc.id)
          and a.version = (select max(version) from `mtech-dc2-prod.wsm.activity` where id = a.id)
          and sts.version = (select max(version) from `mtech-dc2-prod.wsm.activity_status` where id = sts.id)
          and cr.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.container_relationship` where id = cr.id)
       ;;
  }

  measure: count {
    type: count
#     drill_fields: [detail*]
  }

  dimension: wave_number {
    type: string
    primary_key: yes
    sql: ${TABLE}.WaveNumber ;;
  }

  dimension: task_number {
    type: number
    sql: ${TABLE}.TaskNumber ;;
  }

  dimension: task_status {
    type: string
    sql: ${TABLE}.TaskStatus ;;
  }

  dimension: case_number {
    type: string
    sql: ${TABLE}.CaseNumber ;;
  }

  measure: count_of_case_number{
    type: count_distinct
    sql: ${case_number} ;;
  }

  dimension: case_status {
    type: string
    sql: ${TABLE}.CaseStatus ;;
  }

  dimension_group: last_updated {
    type: time
    sql: CAST(${TABLE}.LastUpdated as timestamp) ;;
  }

  dimension: updated_by_user {
    type: string
    sql: ${TABLE}.UpdatedByUser ;;
  }

  dimension: location {
    type: string
    sql: ${TABLE}.location ;;
  }

  dimension: aisle {
    type: string
    sql: ${TABLE}.Aisle ;;
  }

  dimension: area_zone {
    type: string
    sql: ${TABLE}.AreaZone ;;
  }

  dimension: macys_logo {
    type: string
    sql: ${TABLE}.CaseStatus;;
    html: <img src="https://content-az.equisolve.net/_724c7f58341cc8e9580e487fa7ca4cbb/macysinc/db/414/5629/image_thumbnail.png" width="65%" /> ;;
  }

  set: detail {
    fields: [
      wave_number,
      task_number,
      task_status,
      case_number,
      case_status,
      last_updated_time,
      updated_by_user,
      location,
      aisle,
      area_zone
    ]
  }
}
