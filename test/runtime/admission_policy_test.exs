defmodule VialKeeper.Runtime.AdmissionPolicyTest do
  use ExUnit.Case, async: true

  alias VialKeeper.HostConfig
  alias VialKeeper.Runtime.AdmissionPolicy

  @default_keyword AdmissionPolicy.default_keyword()
  @default_limit 128

  describe "defaults" do
    test "default_toml_map matches HostConfig admission defaults" do
      assert AdmissionPolicy.default_toml_map() == HostConfig.defaults()["admission"]
    end

    test "default_keyword matches independent test application config literals" do
      assert Map.new(AdmissionPolicy.default_keyword()) ==
               Map.new(Application.get_env(:vial_keeper, :admission_policy))
    end
  end

  describe "from_keyword/2" do
    test "accepts the canonical default keyword" do
      assert {:ok, policy} = AdmissionPolicy.from_keyword(@default_keyword, @default_limit)
      assert policy.foreground_weight == 8
      assert policy.maintenance_reserved_slots == 1
    end

    test "accepts minimum and maximum weights" do
      for weight <- [1, 64] do
        keyword =
          Keyword.merge(@default_keyword,
            foreground_weight: weight,
            subscription_weight: weight,
            replication_weight: weight,
            maintenance_weight: weight
          )

        assert {:ok, policy} = AdmissionPolicy.from_keyword(keyword, @default_limit)
        assert policy.foreground_weight == weight
      end
    end

    test "rejects weight below minimum" do
      keyword = Keyword.put(@default_keyword, :foreground_weight, 0)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "foreground_weight"
      assert msg =~ "1..64"
    end

    test "rejects weight above maximum" do
      keyword = Keyword.put(@default_keyword, :subscription_weight, 65)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "subscription_weight"
      assert msg =~ "1..64"
    end

    test "rejects non-integer weight" do
      keyword = Keyword.put(@default_keyword, :replication_weight, "2")

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "replication_weight"
    end

    test "rejects negative reserved slot" do
      keyword = Keyword.put(@default_keyword, :foreground_reserved_slots, -1)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "foreground_reserved_slots"
      assert msg =~ "0..128"
    end

    test "rejects reserved slot above admission limit" do
      keyword = Keyword.put(@default_keyword, :subscription_reserved_slots, 200)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "subscription_reserved_slots"
      assert msg =~ "0..128"
    end

    test "rejects reserved slots whose sum exceeds admission limit" do
      keyword =
        Keyword.merge(@default_keyword,
          foreground_reserved_slots: 40,
          subscription_reserved_slots: 40,
          replication_reserved_slots: 40,
          maintenance_reserved_slots: 40
        )

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "reserved slots"
      assert msg =~ "admission_limit"
    end

    test "accepts zero reservations" do
      keyword =
        Keyword.merge(@default_keyword,
          foreground_reserved_slots: 0,
          subscription_reserved_slots: 0,
          replication_reserved_slots: 0,
          maintenance_reserved_slots: 0
        )

      assert {:ok, policy} = AdmissionPolicy.from_keyword(keyword, 8)
      assert AdmissionPolicy.reserved_slots(policy) |> Map.values() |> Enum.sum() == 0
    end

    test "accepts reservations equal to admission limit" do
      keyword =
        Keyword.merge(@default_keyword,
          foreground_reserved_slots: 2,
          subscription_reserved_slots: 2,
          replication_reserved_slots: 2,
          maintenance_reserved_slots: 2
        )

      assert {:ok, policy} = AdmissionPolicy.from_keyword(keyword, 8)
      assert AdmissionPolicy.reserved_slots(policy) |> Map.values() |> Enum.sum() == 8
    end

    test "rejects unknown keys" do
      keyword = Keyword.put(@default_keyword, :bogus_weight, 1)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "unknown key"
    end

    test "rejects missing keys" do
      keyword = Keyword.delete(@default_keyword, :maintenance_weight)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "missing required keys"
    end

    test "rejects duplicate weight keys" do
      keyword =
        [{:foreground_weight, 1}, {:foreground_weight, 64}] ++
          Enum.reject(@default_keyword, fn {key, _} -> key == :foreground_weight end)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "duplicate key"
      assert msg =~ "foreground_weight"
    end

    test "rejects duplicate reserved slot keys" do
      keyword =
        [{:foreground_reserved_slots, 0}, {:foreground_reserved_slots, 1}] ++
          Enum.reject(@default_keyword, fn {key, _} -> key == :foreground_reserved_slots end)

      assert {:error, msg} = AdmissionPolicy.from_keyword(keyword, @default_limit)
      assert msg =~ "duplicate key"
      assert msg =~ "foreground_reserved_slots"
    end
  end
end
