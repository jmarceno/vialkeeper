%{
  configs: [
    %{
      name: "default",
      files: %{
        included: [
          "lib/",
          "src/",
          "test/",
          "web/",
          "apps/",
          "bench/support/"
        ],
        excluded: [~r"/_build/", ~r"/deps/"]
      },
      plugins: [{ExSlop, []}]
    }
  ]
}
