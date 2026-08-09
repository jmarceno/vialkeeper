defmodule ElixirDB.Query.Subscription.Events do
  @moduledoc "Internal stream event shapes for live query subscriptions."

  @type t ::
          snapshot_event()
          | upsert_event()
          | remove_event()
          | reset_event()
          | caught_up_event()
          | heartbeat_event()
          | closed_event()
          | error_event()

  @type snapshot_event :: %{
          type: :snapshot,
          sequence: non_neg_integer(),
          document: map()
        }

  @type upsert_event :: %{
          type: :upsert,
          sequence: non_neg_integer(),
          document: map()
        }

  @type remove_event :: %{
          type: :remove,
          sequence: non_neg_integer(),
          id: binary(),
          revision: binary() | nil
        }

  @type reset_event :: %{type: :reset, sequence: non_neg_integer()}
  @type caught_up_event :: %{type: :caught_up, sequence: non_neg_integer()}
  @type heartbeat_event :: %{type: :heartbeat}
  @type closed_event :: %{type: :closed}
  @type error_event :: %{type: :error, error: ElixirDB.Error.t()}

  @spec snapshot(non_neg_integer(), map()) :: snapshot_event()
  def snapshot(sequence, document),
    do: %{type: :snapshot, sequence: sequence, document: document}

  @spec upsert(non_neg_integer(), map()) :: upsert_event()
  def upsert(sequence, document), do: %{type: :upsert, sequence: sequence, document: document}

  @spec remove(non_neg_integer(), binary(), binary() | nil) :: remove_event()
  def remove(sequence, id, revision),
    do: %{type: :remove, sequence: sequence, id: id, revision: revision}

  @spec caught_up(non_neg_integer()) :: caught_up_event()
  def caught_up(sequence), do: %{type: :caught_up, sequence: sequence}

  @spec reset(non_neg_integer()) :: reset_event()
  def reset(sequence), do: %{type: :reset, sequence: sequence}

  @spec heartbeat() :: heartbeat_event()
  def heartbeat, do: %{type: :heartbeat}

  @spec closed() :: closed_event()
  def closed, do: %{type: :closed}

  @spec error(ElixirDB.Error.t()) :: error_event()
  def error(%ElixirDB.Error{} = error), do: %{type: :error, error: error}

  @spec public(t()) :: map()
  def public(%{type: :snapshot, sequence: sequence, document: document}),
    do: %{"type" => "snapshot", "sequence" => sequence, "document" => public_document(document)}

  def public(%{type: :upsert, sequence: sequence, document: document}),
    do: %{"type" => "upsert", "sequence" => sequence, "document" => public_document(document)}

  def public(%{type: :remove, sequence: sequence, id: id, revision: revision}),
    do: %{"type" => "remove", "sequence" => sequence, "id" => id, "revision" => revision}

  def public(%{type: :reset, sequence: sequence}),
    do: %{"type" => "reset", "sequence" => sequence}

  def public(%{type: :caught_up, sequence: sequence}),
    do: %{"type" => "caught_up", "sequence" => sequence}

  def public(%{type: :heartbeat}), do: %{"type" => "heartbeat"}
  def public(%{type: :closed}), do: %{"type" => "closed"}

  def public(%{type: :error, error: error}),
    do: %{"type" => "error", "error" => ElixirDB.Error.public(error)}

  defp public_document(%{id: id, revision: revision, body: body}),
    do: %{"id" => id, "revision" => revision, "body" => body}

  defp public_document(%{id: id, revision: revision, fields: fields}),
    do: %{"id" => id, "revision" => revision, "fields" => fields}
end
