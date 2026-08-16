# quality:reason ExUnit assert macros and Mix-only quality tools are not Dialyzer contracts
[
  {"test/support/admission_scenario.ex", :unmatched_return},
  {"test/support/storage/contracts/derived.ex", :unmatched_return},
  ~r/quality\/vial_keeper\/reach_smells/,
  ~r/test\/support\/quality_reach_smell_case/
]
