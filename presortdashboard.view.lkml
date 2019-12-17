view: presortdashboard {
  derived_table: {
    datagroup_trigger: macys_datagroup
    partition_keys: ["waveNumber"]
    cluster_keys: ["statusDesc","waveType"]

    sql: --Get Active Waves in Staged, Presort Ready,Presort In Progress, anything else in area
      with WaveData as
      (
        select
          wlc.wave_id --select *
          , case (wlc.status)
                  when 'REQ' THEN 'Requested'
                  when 'ALC' then 'Allocated'
                  when 'RLS' then 'Released'
                  when 'PIKIP' then 'Pick in Progress'
                  when 'SPK'  then 'Staged'
                  when 'PSR'  then 'PreSort Ready'
                  when 'PSIP'  then 'PreSort in Progress'
                  when 'PTSR'  then 'Put to Store Ready'
                  when 'IPK' then 'Pack in Progress'
                  when 'PTSIP' then 'Put to Store in Progress'
                  when 'PIP'  then 'Pack in Progress'
                  when 'SHPIP' then 'Ship in Progress'
                  when 'SHPD' then 'Ship Complete'
                  else wlc.status
                  end status_desc
          , w.total_qty
          , w.order_count
          , wc.wave_type
          , w.wave_nbr
        from
          `mtech-dc2-prod.waving.wave_life_cycle` wlc
          inner join `mtech-dc2-prod.waving.wave` w on w.id = wlc.wave_id
          inner join `mtech-dc2-prod.waving.wave_config` wc on wc.id = w.id
        where
          wlc.id = (select max(id) from `mtech-dc2-prod.waving.wave_life_cycle` where wave_id = wlc.wave_id)
          and wlc.enabled = 1

          and w.version_id = (select min(version_Id) from `mtech-dc2-prod.waving.wave` where id = w.id)
          and w.enabled = 1

          and wc.version_id = (select min(version_Id) from `mtech-dc2-prod.waving.wave_config` where id = wc.id)
          and wc.enabled = 1
      ),
      waveActivity as
      (
      select
        wd.wave_id
        , cast(wd.wave_nbr as string) WaveNumber
        , wd.status_desc
        , wd.total_qty
        , wd.order_count
        , wd.wave_type
        , min(case when wlc.status = 'REQ' THEN wlc.updated_time else null end) Requested
        , min(case when wlc.status = 'ALC' then wlc.updated_time else null end) Allocated
        , min(case when wlc.status = 'RLS' then wlc.updated_Time else null end) Released
        , min(case when wlc.status = 'PIKIP' then wlc.updated_time else null end) Pick_In_Progress
        , min(case when wlc.status = 'SPK'  then wlc.updated_time else null end) Staged
        , min(case when wlc.status = 'PSR'  then wlc.updated_time else null end) PreSortReady
        , min(case when wlc.status = 'PSIP'  then wlc.updated_time else null end) PreSortInProgress
        , min(case when wlc.status = 'PTSR'  then wlc.updated_time else null end) PutToStoreReady
        , min(case when wlc.status = 'PTSIP' then wlc.updated_time else null end) PutToStoreInProgress
        , min(case when wlc.status = 'SHPIP' then wlc.updated_time else null end) ShipInProgress
        , min(case when wlc.status = 'SHPD' then wlc.updated_time else null end) ShipComplete
        , min(case when wlc.status = 'PIP'  then wlc.updated_time else null end) PackInProgress
      from
        `mtech-dc2-prod.waving.wave_life_cycle` wlc
        inner join WaveData wd on wd.wave_id = wlc.wave_id
      where
        wlc.id = (select min(id) from `mtech-dc2-prod.waving.wave_life_cycle` where wave_id = wlc.wave_id and status = wlc.status)
      group by
        wd.wave_id
        , wd.wave_nbr
        , wd.status_desc
        , wd.total_Qty
        , wd.order_count
        , wd.wave_type
      ),
      WavesInPreSort as (
      select
        cea.attribute_value waveNumber
      from
       `mtech-dc2-prod.inventory.entity` e
        inner join `mtech-dc2-prod.inventory.common_entity_attributes` cea
          on cea.entity_id = e.id
          and cea.attribute_name = 'WaveNumber'
          and cea.enabled = 1
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
        and cea.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.common_entity_attributes` where id = cea.id)
        and cr.version_id = (select max(version_id) from `mtech-dc2-prod.inventory.container_relationship` where id = cr.id)
        and cr.enabled = 1
      group by cea.attribute_value
      )

      ,
      --Get Total Cases and Staged Cases for waves in RLS, PIKIP, SPK, PSR, PSIP
      StagedCases as
      (
      select
        waveNumber
        , Count(distinct caseNumber) TotalCases
        , sum(units) TotalQty
        , count(distinct stagedCases) StagedCases
        , sum(stagedUnits) StagedUnits
      from
      (
      Select
        cea.attribute_value waveNumber
        , e.entity_id caseNumber
        , ss.item
        , max(ss.quantity) Units
        , Min(case when e.entity_status = 'SPK' then e.entity_id else null end) stagedCases
        , Max(case when e.entity_status = 'SPK' then ss.quantity else 0 end) stagedUnits
      from
        `mtech-dc2-prod.inventory.entity` e
        inner join `mtech-dc2-prod.inventory.common_entity_attributes` cea
          on cea.entity_id = e.id
          and cea.attribute_name = 'WaveNumber'
        inner join `mtech-dc2-prod.inventory.inventory_snapshot` ss
          on ss.container = e.entity_id
          and DATETIME_TRUNC(DATETIME(ss.created_time), MINUTE) = DATETIME_TRUNC(DATETIME(e.created_time), MINUTE)
          and ss.versioN_id = 0
      where
        e.entity_Type = 'CSE'
        and e.entity_status in ('CRE','SPK')
        and e.version_id = (select min(Version_id) from `mtech-dc2-prod.inventory.entity` where id = e.id and entity_Status = e.entity_Status)
        and cea.version_id = (select min(version_id) from `mtech-dc2-prod.inventory.common_entity_attributes` where id = cea.id)
      group by
        cea.attribute_value
        , e.entity_id
        , ss.item
      ) group by waveNumber
      )
      , sortedUnits as
      (
      Select
      waveNumber
      , sum(units) SortedUnits
      From
      (
      select
        cea.attribute_value waveNumber
        , e.entity_id tote
        , ss.item UPC
        , max(ss.quantity) units
      from
        `mtech-dc2-prod.inventory.entity` e
        inner join `mtech-dc2-prod.inventory.common_entity_attributes` cea
          on cea.entity_id = e.id
          and cea.attribute_name = 'WaveNumber'
        inner join `mtech-dc2-prod.inventory.inventory_snapshot` ss
          on ss.container = e.entity_id
          and DATETIME_TRUNC(DATETIME(ss.created_time), MINUTE) = DATETIME_TRUNC(DATETIME(e.created_time), MINUTE)
      where
        e.entity_type = 'TOTE'
        and e.version_id = 0
      group by
        cea.attribute_value
        , e.entity_id
        , ss.item
      )
      group by
      waveNumber
      )

      Select
        cast(a.waveNumber as string) waveNumber
        , status_desc statusDesc
        , totalQty
        , order_count orderCount
        , wave_type waveType
        , Allocated allocated
        , Released released
        , Pick_In_Progress pickInProgress
        , Staged staged
        , PreSortReady preSortReady
        , PreSortInProgress preSortInProgress
        , PutToStoreReady putToStoreReady
        , PutToStoreInProgress putToStoreInProgress
        , TotalCases totalCases
        , StagedCases stagedCases
        , StagedUnits stagedUnits
        , SortedUnits sortedUnits
      from
        WaveActivity a
        inner join WavesInPreSort ps on ps.waveNumber = a.waveNumber
        left join stagedCases s on s.waveNumber = a.waveNumber
        left join sortedUnits srt on srt.waveNumber = a.waveNumber
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

  dimension: status_desc {
    type: string
    sql: ${TABLE}.statusDesc ;;
  }

  dimension: total_qty {
    type: number
    sql: ${TABLE}.totalQty ;;
  }

  dimension: order_count {
    type: number
    sql: ${TABLE}.orderCount ;;
  }

  dimension: wave_type {
    type: string
    sql: ${TABLE}.waveType ;;
  }

  dimension_group: allocated {
    type: time
    sql: ${TABLE}.allocated ;;
  }

  dimension_group: released {
    type: time
    sql: ${TABLE}.released ;;
  }

  dimension_group: pick_in_progress {
    type: time
    sql: ${TABLE}.pickInProgress ;;
  }

  dimension_group: staged {
    type: time
    sql: ${TABLE}.staged ;;
  }

  dimension_group: pre_sort_ready {
    type: time
    sql: ${TABLE}.preSortReady ;;
  }

  dimension_group: pre_sort_in_progress {
    type: time
    sql: ${TABLE}.preSortInProgress ;;
  }

  dimension_group: put_to_store_ready {
    type: time
    sql: ${TABLE}.putToStoreReady ;;
  }

  dimension_group: put_to_store_in_progress {
    type: time
    sql: ${TABLE}.putToStoreInProgress ;;
  }

  dimension: total_cases {
    type: number
    sql: ${TABLE}.totalCases ;;
  }

  dimension: staged_cases {
    type: number
    sql: ${TABLE}.stagedCases ;;
  }

  dimension: staged_units {
    type: number
    sql: ${TABLE}.stagedUnits ;;
  }

  dimension: sorted_units {
    type: number
    sql: ${TABLE}.sortedUnits ;;
  }

  set: detail {
    fields: [
      wave_number,
      status_desc,
      total_qty,
      order_count,
      wave_type,
      allocated_time,
      released_time,
      pick_in_progress_time,
      staged_time,
      pre_sort_ready_time,
      pre_sort_in_progress_time,
      put_to_store_ready_time,
      put_to_store_in_progress_time,
      total_cases,
      staged_cases,
      staged_units,
      sorted_units
    ]
  }
}
