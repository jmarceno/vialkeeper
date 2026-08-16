defmodule VialKeeper.Shadow.LocalEndpoint do
  @moduledoc "Shadow endpoint that invokes worker services without HTTP."

  @behaviour VialKeeper.Shadow.Endpoint

  alias VialKeeper.Error
  alias VialKeeper.Shadow.Worker

  defstruct [:worker, :worker_options, :timeout]

  @type t :: %__MODULE__{worker: module(), worker_options: keyword(), timeout: timeout()}

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs \\ %{}) do
    attrs = if is_list(attrs), do: Map.new(attrs), else: attrs

    if is_map(attrs) do
      worker = Map.get(attrs, :worker, Map.get(attrs, "worker", Worker))
      options = Map.get(attrs, :worker_options, Map.get(attrs, "worker_options", []))

      if is_atom(worker) and is_list(options),
        do:
          {:ok,
           %__MODULE__{
             worker: worker,
             worker_options: options,
             timeout: Map.get(attrs, :timeout, VialKeeper.Config.request_timeout_ms())
           }},
        else: {:error, Error.invalid_request("local shadow endpoint is invalid")}
    else
      {:error, Error.invalid_request("local shadow endpoint is invalid")}
    end
  end

  @impl true
  def capabilities(%__MODULE__{} = endpoint, _timeout),
    do: invoke(endpoint, :capabilities, [])

  @impl true
  def provision(%__MODULE__{} = endpoint, request, _timeout),
    do: invoke(endpoint, :provision, [request])

  @impl true
  def inspect(%__MODULE__{} = endpoint, request, _timeout),
    do: invoke(endpoint, :inspect, [request])

  @impl true
  def destroy(%__MODULE__{} = endpoint, request, _timeout),
    do: invoke(endpoint, :destroy, [request])

  @impl true
  def read_document(%__MODULE__{} = endpoint, request, _timeout, opts),
    do: invoke(endpoint, :read_document, [request, opts])

  @impl true
  def bulk_read_documents(%__MODULE__{} = endpoint, request, _timeout, opts),
    do: invoke(endpoint, :bulk_read_documents, [request, opts])

  @impl true
  def open_attachment_representation(%__MODULE__{} = endpoint, request, _timeout, opts),
    do: invoke(endpoint, :open_attachment_representation, [request, opts])

  defp invoke(%__MODULE__{worker: worker, worker_options: options}, function, args) do
    apply(worker, function, Enum.concat(args, [options]))
  rescue
    exception in [ArgumentError, FunctionClauseError, UndefinedFunctionError] ->
      {:error,
       Error.internal_error("local shadow endpoint invocation failed", %{cause: inspect(exception)})}
  end
end
