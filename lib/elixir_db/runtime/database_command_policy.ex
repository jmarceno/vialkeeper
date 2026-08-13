defmodule ElixirDB.Runtime.DatabaseCommandPolicy do
  @moduledoc "Explicit command authorization for ordinary, derived, and shadow databases."

  alias ElixirDB.Commands
  alias ElixirDB.DatabaseKind
  alias ElixirDB.Error
  alias ElixirDB.Runtime.CommandContext

  @shadow_read MapSet.new([
                 Commands.GetDocument,
                 Commands.GetRevision,
                 Commands.ResolveAttachmentTicket,
                 Commands.ResolveBlobMetadata
               ])

  @shadow_replication MapSet.new([
                        Commands.Identity,
                        Commands.ReadChanges,
                        Commands.DiffRevisions,
                        Commands.GetRevisionChains,
                        Commands.ImportRevisionChains,
                        Commands.GetRevisionsBatch,
                        Commands.GetCheckpoint,
                        Commands.PutCheckpoint,
                        Commands.GetLocalRecord,
                        Commands.PutLocalRecord,
                        Commands.ReadBoundaryPages,
                        Commands.InstallBoundaryPages
                      ])

  @shadow_control MapSet.new([
                    Commands.Identity,
                    Commands.IntegrityCheck,
                    Commands.Close
                  ])

  @spec authorize(DatabaseKind.t(), CommandContext.t(), struct()) :: :ok | {:error, Error.t()}
  def authorize(database_kind, %CommandContext{} = context, command) do
    normalized_kind = normalize_kind(database_kind)
    command_type = command_type(command)

    cond do
      normalized_kind != :shadow ->
        :ok

      allowed?(context.class, command_type, command) ->
        :ok

      true ->
        {:error,
         Error.shadow_command_forbidden("command is not allowed for a shadow database", %{
           command: inspect(command_type),
           context: context.class
         })}
    end
  end

  @spec allowed?(CommandContext.class(), module(), term()) :: boolean()
  def allowed?(:shadow_read, command_type, command) do
    MapSet.member?(@shadow_read, command_type) or
      (command_type == Commands.GetLocalRecord and valid_shadow_state_read?(command))
  end

  def allowed?(:shadow_replication, command_type, command) do
    MapSet.member?(@shadow_replication, command_type) and valid_checkpoint_command?(command)
  end

  def allowed?(:shadow_control, command_type, _command),
    do: MapSet.member?(@shadow_control, command_type)

  def allowed?(_, _command_type, _command), do: false

  @spec classify(struct()) :: :shadow_read | :shadow_replication | :shadow_control | :unknown
  def classify(command) do
    type = command_type(command)

    cond do
      MapSet.member?(@shadow_read, type) -> :shadow_read
      MapSet.member?(@shadow_replication, type) -> :shadow_replication
      MapSet.member?(@shadow_control, type) -> :shadow_control
      true -> :unknown
    end
  end

  @spec command_type(term()) :: module() | :unknown
  def command_type(%module{}), do: module
  def command_type(_), do: :unknown

  @doc "Returns the closed shadow policy table keyed by every command envelope type."
  @spec shadow_policy() :: %{module() => [CommandContext.class()]}
  def shadow_policy do
    Enum.into(Commands.command_types(), %{}, fn command_type ->
      classes =
        [:shadow_read, :shadow_replication, :shadow_control]
        |> Enum.filter(&MapSet.member?(policy_set(&1), command_type))

      {command_type, classes}
    end)
  end

  defp valid_checkpoint_command?(%Commands.GetLocalRecord{namespace: namespace}),
    do: namespace in ["shadow_checkpoints", "shadow_checkpoint", "retention_boundary_state"]

  defp valid_checkpoint_command?(%Commands.PutLocalRecord{request: request}) when is_map(request) do
    Map.get(request, :namespace, Map.get(request, "namespace")) in [
      "shadow_checkpoints",
      "shadow_checkpoint"
    ]
  end

  defp valid_checkpoint_command?(_), do: true

  defp valid_shadow_state_read?(%Commands.GetLocalRecord{
         namespace: "shadow_state",
         key: "watermark"
       }),
       do: true

  defp valid_shadow_state_read?(_), do: false

  defp policy_set(:shadow_read), do: @shadow_read
  defp policy_set(:shadow_replication), do: @shadow_replication
  defp policy_set(:shadow_control), do: @shadow_control

  defp normalize_kind(kind) do
    case DatabaseKind.normalize(kind) do
      {:ok, normalized} -> normalized
      _ -> kind
    end
  end
end
