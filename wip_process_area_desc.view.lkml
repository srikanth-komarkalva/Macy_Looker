view: wip_process_area_desc {
  derived_table: {
    sql: select "BTY" as Proc_Area_Short_Desc, "Beauty" as Proc_Area_Long_Desc
      UNION ALL
      select "BLK" as Proc_Area_Short_Desc, "Bulk" as Proc_Area_Long_Desc
      UNION ALL
      select "GOR" as Proc_Area_Short_Desc, "Gourmet" as Proc_Area_Long_Desc
      UNION ALL
      select "FRG" as Proc_Area_Short_Desc, "Fragile" as Proc_Area_Long_Desc
      UNION ALL
      select "JWL" as Proc_Area_Short_Desc, "Jewelry" as Proc_Area_Long_Desc
      UNION ALL
      select "OVR" as Proc_Area_Short_Desc, "Oversize" as Proc_Area_Long_Desc
      UNION ALL
      select "PTC" as Proc_Area_Short_Desc, "Pick_To_Carton" as Proc_Area_Long_Desc
      UNION ALL
      select "SPK" as Proc_Area_Short_Desc, "Store_Pack" as Proc_Area_Long_Desc
      UNION ALL
      select "BYP" as Proc_Area_Short_Desc, "Bypass" as Proc_Area_Long_Desc
      UNION ALL
      select "CDP" as Proc_Area_Short_Desc, "CDP" as Proc_Area_Long_Desc
      UNION ALL
      select "OSC" as Proc_Area_Short_Desc, "Open_Sort_Count" as Proc_Area_Long_Desc
      UNION ALL
      select "UNK" as Proc_Area_Short_Desc, "Unknown" as Proc_Area_Long_Desc
      UNION ALL
      select "HAF" as Proc_Area_Short_Desc, "Hold&Flow" as Proc_Area_Long_Desc
      UNION ALL
      select "BKG" as Proc_Area_Short_Desc, "Backstage" as Proc_Area_Long_Desc
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: proc_area_short_desc {
    type: string
    sql: ${TABLE}.Proc_Area_Short_Desc ;;
  }

  dimension: proc_area_long_desc {
    label: "Process"
    type: string
    sql: ${TABLE}.Proc_Area_Long_Desc ;;
  }

  set: detail {
    fields: [proc_area_short_desc, proc_area_long_desc]
  }
}
