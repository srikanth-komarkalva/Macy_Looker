include: "Cognos_Reports.model.lkml"
include: "digital_executive_summary_testing.view.lkml"

view: digital_executive_summary_prod_b {
  extends: [digital_executive_summary_testing]
}
