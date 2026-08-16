defmodule VialKeeper.Bench.SimpleWikiTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Bench.{SimpleWiki, Tmp}

  test "parses current main-namespace pages and rejects redirects" do
    page = """
    <page><title>One &amp; Two</title><ns>0</ns><id>42</id><revision><text xml:space="preserve">hello &amp; world</text></revision></page>
    """

    assert {:ok, article} = SimpleWiki.parse_page(page)
    assert article["page_id"] == "42"
    assert article["title"] == "One & Two"
    assert article["text"] == "hello & world"

    refute match?({:ok, _}, SimpleWiki.parse_page("<page><ns>0</ns><id>43</id><redirect /></page>"))

    refute match?(
             {:ok, _},
             SimpleWiki.parse_page("<page><ns>1</ns><id>44</id><text>x</text></page>")
           )
  end

  test "generates a smoke fixture from one bzip2 archive" do
    root = Tmp.dir("simplewiki")
    xml = Path.join(root, "pages.xml")
    archive = Path.join(root, "pages.xml.bz2")
    staging = Path.join(root, "staging")

    File.write!(xml, """
    <mediawiki>
      <page><title>One</title><ns>0</ns><id>1</id><revision><text>alpha beta</text></revision></page>
      <page><title>Two</title><ns>0</ns><id>2</id><revision><text>beta gamma</text></revision></page>
      <page><title>Redirect</title><ns>0</ns><id>3</id><redirect title="One" /></page>
      <page><title>Talk</title><ns>1</ns><id>4</id><revision><text>not selected</text></revision></page>
      <page><title>Three</title><ns>0</ns><id>5</id><revision><text>gamma delta</text></revision></page>
      <page><title>Four</title><ns>0</ns><id>6</id><revision><text>delta epsilon</text></revision></page>
    </mediawiki>
    """)

    {compressed, 0} = System.cmd("bzip2", ["-c", xml])
    File.write!(archive, compressed)

    assert {:ok, manifest} =
             SimpleWiki.generate_fixture(
               archive,
               %{"version" => "v1", "source_url" => "local"},
               :smoke,
               staging
             )

    assert [_, _, _] = manifest["articles"]
    assert manifest["attachment_count"] == 1
    assert manifest["attachment_bytes"] == 4096
    assert manifest["query_workload_version"] == "simplewiki-query-v2"
    assert File.regular?(Path.join([staging, "objects", "wiki-1.1", "wiki-1.1.txt"]))
    assert File.stat!(Path.join([staging, "objects", "wiki-1.1", "attachment-0.bin"])).size == 4096

    assert {:ok, workload} = SimpleWiki.load_query_workload(staging)
    assert workload["query_workload_version"] == "simplewiki-query-v2"
    assert "beta" in workload["categories"]["common"]
    assert workload["categories"]["zero_match"] == ["zzzzzxxyy-no-such-term"]
  end
end
