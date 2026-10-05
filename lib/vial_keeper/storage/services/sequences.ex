defmodule VialKeeper.Storage.Services.Sequences do
  @moduledoc """
  Sequence reservations held by a writing process, and the visible-sequence
  overlay applied to every identity read.

  The runtime `VialKeeper.Runtime.SequenceLedger` reserves a contiguous range
  of sequences for a write before its transaction begins and stores it in the
  writing process with `put_reservation/4`. Storage services take numbers from
  that range with `take/2`; no storage transaction allocates a sequence on its
  own. Numbers a write does not use become permanent holes.

  Storage used without a running ledger (offline tooling and storage-level
  callers) has one writer and nothing outstanding. There `take/2` raises the
  reserved-through value inside the caller's own write transaction through
  `persist_sequence_reservation`, so a rolled-back write leaves no hole and the
  reserved-through value is exactly the visible sequence.
  """

  alias VialKeeper.DerivedView.Engine
  alias VialKeeper.Error
  alias VialKeeper.MapAccess
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Access

  @reservation_key :vial_keeper_sequence_reservation
  @view_key :vial_keeper_sequence_view
  @visible_slot 1
  @version_slot 2

  @type reservation :: %{
          token: reference(),
          next: pos_integer(),
          last: pos_integer(),
          max_used: non_neg_integer()
        }

  @typedoc """
  The visible watermark, the data version, and the data version the serving
  ledger started its run at (`data_version_base`). Every data version of an
  earlier run is below the base.
  """
  @type view :: %{
          visible: non_neg_integer(),
          data_version: pos_integer(),
          data_version_base: pos_integer()
        }

  @doc "Stores a reserved range `first..last` for `database_uuid` in this process."
  @spec put_reservation(binary(), reference(), pos_integer(), pos_integer()) :: :ok
  def put_reservation(database_uuid, token, first, last)
      when is_binary(database_uuid) and is_reference(token) and is_integer(first) and
             is_integer(last) and first > 0 and last >= first do
    _ =
      Process.put({@reservation_key, database_uuid}, %{
        token: token,
        next: first,
        last: last,
        max_used: 0
      })

    :ok
  end

  @doc "Removes and returns this process's reservation for `database_uuid`."
  @spec pop_reservation(binary()) :: reservation() | nil
  def pop_reservation(database_uuid) when is_binary(database_uuid),
    do: Process.delete({@reservation_key, database_uuid})

  @doc """
  Takes `count` sequences for a write transaction on `context`.

  Numbers come from this process's ledger reservation. A missing or exhausted
  reservation is an internal error while the database's ledger runs; without
  a ledger the numbers are reserved inside the caller's transaction.
  """
  @spec take(BackendContext.t(), non_neg_integer()) :: {:ok, [pos_integer()]} | {:error, Error.t()}
  def take(%BackendContext{}, 0), do: {:ok, []}

  def take(%BackendContext{} = context, count) when is_integer(count) and count > 0 do
    uuid = database_uuid(context)
    key = {@reservation_key, uuid}

    case Process.get(key) do
      %{next: next, last: last} = reservation when next + count - 1 <= last ->
        used = next + count - 1
        _ = Process.put(key, %{reservation | next: used + 1, max_used: used})
        {:ok, Enum.to_list(next..used)}

      nil ->
        if ledger_view(uuid) == :none,
          do: take_standalone(context, count),
          else: {:error, missing_reservation()}

      _exhausted ->
        {:error, missing_reservation()}
    end
  end

  @doc """
  Sets `current_sequence` (the visible watermark) and `data_version` on a
  stored identity.

  The context's `sequence_view` wins when present; otherwise the running
  ledger's view is used, and without a ledger the persisted reserved-through
  value.
  """
  @spec overlay(BackendContext.t(), map()) :: {:ok, map()} | {:error, Error.t()}
  def overlay(%BackendContext{} = context, identity) when is_map(identity) do
    with {:ok, %{visible: visible, data_version: version, data_version_base: base}} <-
           view(context) do
      {:ok,
       identity
       |> Map.put(:current_sequence, visible)
       |> Map.put(:data_version, version)
       |> Map.put(:data_version_base, base)}
    end
  end

  @doc "Returns the visible sequence and data version for the context's database."
  @spec view(BackendContext.t()) :: {:ok, view()} | {:error, Error.t()}
  def view(%BackendContext{sequence_view: %{visible: _, data_version: _} = view}), do: {:ok, view}

  def view(%BackendContext{} = context) do
    case ledger_view(database_uuid(context)) do
      {:ok, view} -> {:ok, view}
      :none -> standalone_view(context)
    end
  end

  @doc "Creates the cell a sequence ledger publishes its view in."
  @spec new_view_cell() :: :atomics.atomics_ref()
  def new_view_cell, do: :atomics.new(2, signed: false)

  @doc "Stores the visible watermark and data version in a view cell."
  @spec store_view(:atomics.atomics_ref(), non_neg_integer(), pos_integer()) :: :ok
  def store_view(cell, visible, data_version) do
    :ok = :atomics.put(cell, @visible_slot, visible)
    :atomics.put(cell, @version_slot, data_version)
  end

  @doc """
  Publishes the view cell of the ledger process `ledger` for a database. It
  is written once per ledger initialization; the cell itself changes on every
  completed write without a message.
  """
  @spec publish_view(binary(), pid(), :atomics.atomics_ref(), pos_integer()) :: :ok
  def publish_view(database_uuid, ledger, cell, data_version_base)
      when is_binary(database_uuid) and is_pid(ledger) and is_integer(data_version_base),
      do: :persistent_term.put({@view_key, database_uuid}, {ledger, cell, data_version_base})

  @doc "Withdraws the view `ledger` published for a database."
  @spec withdraw_view(binary(), pid()) :: :ok
  def withdraw_view(database_uuid, ledger) when is_binary(database_uuid) and is_pid(ledger) do
    case :persistent_term.get({@view_key, database_uuid}, nil) do
      {^ledger, _cell, _base} ->
        _ = :persistent_term.erase({@view_key, database_uuid})
        :ok

      _other ->
        :ok
    end
  end

  @doc """
  Returns the visible watermark and data version published by the running
  ledger of a database, or `:none` when no live ledger serves it.
  """
  @spec ledger_view(binary() | nil) :: {:ok, view()} | :none
  def ledger_view(database_uuid) when is_binary(database_uuid) do
    case :persistent_term.get({@view_key, database_uuid}, nil) do
      {ledger, cell, base} ->
        if Process.alive?(ledger),
          do:
            {:ok,
             %{
               visible: :atomics.get(cell, @visible_slot),
               data_version: :atomics.get(cell, @version_slot),
               data_version_base: base
             }},
          else: :none

      nil ->
        :none
    end
  end

  def ledger_view(_database_uuid), do: :none

  @doc "Upper bound of sequences a bulk write request may use."
  @spec bulk_bound(map()) :: non_neg_integer()
  def bulk_bound(request) when is_map(request) do
    case MapAccess.get(request, :operations) do
      operations when is_list(operations) -> length(operations)
      _other -> 0
    end
  end

  @doc "Upper bound of sequences a revision-chain import request may use."
  @spec import_bound(map()) :: non_neg_integer()
  def import_bound(request) when is_map(request) do
    request
    |> list_field(:chains)
    |> Enum.concat(list_field(request, :purged_boundaries))
    |> Enum.map(&document_id/1)
    |> Enum.uniq()
    |> length()
  end

  @doc """
  Upper bound of sequences a derived source batch or rebuild page may use.

  Each removal changes at most one output document. Each row changes at most
  its old and its new output document (or reducer group).
  """
  @spec derived_batch_bound(map()) :: non_neg_integer()
  def derived_batch_bound(request) when is_map(request),
    do: 2 * length(list_field(request, :rows)) + length(list_field(request, :removals))

  @doc "Upper bound of sequences one derived stale-prune page may use."
  @spec derived_prune_bound(map()) :: non_neg_integer()
  def derived_prune_bound(request) when is_map(request) do
    case Engine.rebuild_page_limit(request) do
      {:ok, limit} -> limit
      {:error, _invalid_limit_fails_the_command} -> 0
    end
  end

  defp take_standalone(context, count) do
    port = Access.port(context, :change_log)

    with {:ok, high_water} <- port.sequence_high_water(context),
         :ok <- port.persist_sequence_reservation(context, high_water + count) do
      {:ok, Enum.to_list((high_water + 1)..(high_water + count))}
    end
  end

  defp standalone_view(context) do
    with {:ok, high_water} <- high_water(context) do
      {:ok, %{visible: high_water, data_version: high_water + 1, data_version_base: high_water + 1}}
    end
  end

  @doc """
  Returns the persisted reserved-through value; 0 for a backend without a
  change log, which never stores sequences.
  """
  @spec high_water(BackendContext.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def high_water(%BackendContext{} = context) do
    if Access.available?(context, :change_log),
      do: Access.port(context, :change_log).sequence_high_water(context),
      else: {:ok, 0}
  end

  defp database_uuid(%BackendContext{identity: identity}),
    do: MapAccess.get(identity || %{}, :database_uuid)

  defp list_field(request, key) do
    case MapAccess.get(request, key) do
      values when is_list(values) -> values
      _other -> []
    end
  end

  defp document_id(entry) when is_map(entry), do: MapAccess.get(entry, :document_id)
  defp document_id(_entry), do: nil

  defp missing_reservation,
    do: Error.internal_error("sequence reservation missing or exhausted")
end
