defmodule VialKeeper.Retention.SafeReportTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Retention.SafeReport

  @base %{
    source_history_epoch: "epoch-a",
    checkpoint_source_history_epoch: "epoch-a",
    source_sequence: 40,
    previous_safe_source_sequence: 20,
    proposed_safe_source_sequence: 40,
    source_compaction_epoch: 3,
    installed_source_compaction_epoch: 3,
    boundaries_installed_through: 3,
    position_durably_applied: true,
    has_unacknowledged_local_mutations: false,
    checkpoint_only: false
  }

  test "advances when all causal gates pass" do
    assert %{safe_source_sequence: 40, advanced: true, reason: :advanced} =
             SafeReport.decide(@base)
  end

  test "checkpoint-only batch does not advance safe" do
    assert %{safe_source_sequence: 20, advanced: false, reason: :checkpoint_only} =
             SafeReport.decide(Map.put(@base, :checkpoint_only, true))
  end

  test "epoch mismatch holds previous safe position" do
    assert %{safe_source_sequence: 20, advanced: false, reason: :epoch_mismatch} =
             SafeReport.decide(Map.put(@base, :checkpoint_source_history_epoch, "epoch-old"))
  end

  test "non-monotonic safe report is rejected" do
    assert %{safe_source_sequence: 20, advanced: false, reason: :non_monotonic} =
             SafeReport.decide(Map.put(@base, :proposed_safe_source_sequence, 15))
  end

  test "local mutations block safe advancement" do
    assert %{safe_source_sequence: 20, advanced: false, reason: :local_mutations_pending} =
             SafeReport.decide(Map.put(@base, :has_unacknowledged_local_mutations, true))
  end

  test "missing boundary install blocks safe advancement" do
    assert %{safe_source_sequence: 20, advanced: false, reason: :boundaries_incomplete} =
             SafeReport.decide(Map.put(@base, :boundaries_installed_through, 1))
  end
end
