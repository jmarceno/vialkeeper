%Doctor.Config{
  # AdapterFacade and LifecycleHelpers only quote `def`s into adapters/ports.
  # Doctor still walks those quote blocks as functions of the helper modules,
  # including `unquote(name)` nodes that cannot carry BEAM @spec entries.
  ignore_modules: [
    VialKeeper.Storage.AdapterFacade,
    VialKeeper.Storage.Ports.LifecycleHelpers
  ],
  ignore_paths: [
    ~r/^test\//,
    ~r/^bench\//,
    ~r/^quality\//
  ],
  min_module_doc_coverage: 0,
  min_overall_doc_coverage: 0,
  min_overall_moduledoc_coverage: 100,
  min_module_spec_coverage: 80,
  min_overall_spec_coverage: 90,
  exception_moduledoc_required: true,
  struct_type_spec_required: true,
  raise: true,
  reporter: Doctor.Reporters.Summary,
  umbrella: false
}
