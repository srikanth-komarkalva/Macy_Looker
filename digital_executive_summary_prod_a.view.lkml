include: "Cognos_Reports.model.lkml"


include: "digital_executive_summary_testing.view.lkml"

view: digital_executive_summary_prod_a {
  extends: [digital_executive_summary_testing]
}
