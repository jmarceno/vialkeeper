defmodule ElixirDB.Storage.Commands do
  @moduledoc """
  Typed command envelopes used at the database-owner boundary.

  Public catalog/document callers may still pass tagged `{:command, ...}` tuples;
  `normalize/1` converts those into these structs before `DatabaseOwner` dispatches.
  """

  defmodule Identity do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule UpdateConfig do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule IntegrityCheck do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule GetDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetRevision do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule PutDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule CreateDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DeleteDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ResolveConflict do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule BulkWrite do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ReadChanges do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DiffRevisions do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetRevisionChains do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ImportRevisionChains do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetCheckpoint do
    @moduledoc false
    @enforce_keys [:replication_id]
    defstruct [:replication_id]
  end

  defmodule PutCheckpoint do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetLocalRecord do
    @moduledoc false
    @enforce_keys [:namespace, :key]
    defstruct [:namespace, :key]
  end

  defmodule PutLocalRecord do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ListIndexes do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule CreateIndex do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DeleteIndex do
    @moduledoc false
    @enforce_keys [:index_id]
    defstruct [:index_id]
  end

  defmodule RebuildIndex do
    @moduledoc false
    @enforce_keys [:index_id]
    defstruct [:index_id]
  end

  defmodule ExecuteQuery do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ExplainQuery do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ListJobs do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule PutJob do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DeleteJob do
    @moduledoc false
    @enforce_keys [:job_id]
    defstruct [:job_id]
  end

  defmodule Close do
    @moduledoc false
    defstruct []
  end

  @doc """
  Converts tagged owner tuples (and already-normalized structs) into command structs.
  """
  @spec normalize(term()) :: struct() | term()
  def normalize(%_{} = command), do: command

  def normalize({:command, :identity, request}), do: %Identity{request: request || %{}}
  def normalize({:command, :update_config, request}), do: %UpdateConfig{request: request}
  def normalize({:command, :integrity_check, request}), do: %IntegrityCheck{request: request || %{}}
  def normalize({:command, :get_document, request}), do: %GetDocument{request: request}
  def normalize({:command, :get_revision, request}), do: %GetRevision{request: request}
  def normalize({:command, :put, request}), do: %PutDocument{request: request}
  def normalize({:command, :delete, request}), do: %DeleteDocument{request: request}
  def normalize({:command, :bulk_write, request}), do: %BulkWrite{request: request}
  def normalize({:command, :resolve, request}), do: %ResolveConflict{request: request}
  def normalize({:command, :read_changes, request}), do: %ReadChanges{request: request}
  def normalize({:command, :diff_revisions, request}), do: %DiffRevisions{request: request}

  def normalize({:command, :get_revision_chains, request}),
    do: %GetRevisionChains{request: request}

  def normalize({:command, :import_revision_chains, request}),
    do: %ImportRevisionChains{request: request}

  def normalize({:command, :get_local_record, "checkpoints", replication_id}),
    do: %GetCheckpoint{replication_id: replication_id}

  def normalize({:command, :get_local_record, namespace, key}),
    do: %GetLocalRecord{namespace: namespace, key: key}

  def normalize({:command, :put_local_record, %{namespace: "checkpoints"} = request}),
    do: %PutCheckpoint{request: request}

  def normalize({:command, :put_local_record, %{"namespace" => "checkpoints"} = request}),
    do: %PutCheckpoint{request: request}

  def normalize({:command, :put_local_record, request}), do: %PutLocalRecord{request: request}
  def normalize({:command, :list_indexes, request}), do: %ListIndexes{request: request || %{}}
  def normalize({:command, :create_index, request}), do: %CreateIndex{request: request}
  def normalize({:command, :delete_index, index_id}), do: %DeleteIndex{index_id: index_id}
  def normalize({:command, :rebuild_index, index_id}), do: %RebuildIndex{index_id: index_id}
  def normalize({:command, :query, request}), do: %ExecuteQuery{request: request}
  def normalize({:command, :explain_query, request}), do: %ExplainQuery{request: request}
  def normalize({:command, :list_jobs, request}), do: %ListJobs{request: request || %{}}
  def normalize({:command, :put_job, request}), do: %PutJob{request: request}
  def normalize({:command, :delete_job, job_id}), do: %DeleteJob{job_id: job_id}
  def normalize({:command, :close}), do: %Close{}
  def normalize(other), do: other
end
