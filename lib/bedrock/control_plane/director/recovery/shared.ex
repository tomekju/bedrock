defmodule Bedrock.ControlPlane.Director.Recovery.Shared do
  @moduledoc """
  Shared utilities for recovery phase modules.
  """

  alias Bedrock.Service.Worker

  @type starter_fn :: (Supervisor.child_spec(), node() ->
                         {:ok, pid()}
                         | {:error, {:supervisor_exit, term()}}
                         | {:error, {:supervisor_error, term()}}
                         | {:error, {:unexpected_failure, term()}}
                         | {:error, :already_started}
                         | {:error, :max_children}
                         | {:error, term()})

  @doc """
  Creates a starter function for supervised processes.

  Returns a function that can start child processes on specific nodes
  using the given supervisor OTP name.
  """
  @spec starter_for(atom()) :: starter_fn()
  def starter_for(supervisor_otp_name) do
    fn child_spec, node ->
      try do
        {supervisor_otp_name, node}
        |> DynamicSupervisor.start_child(child_spec)
        |> case do
          {:ok, pid} -> {:ok, pid}
          {:ok, pid, _} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
      catch
        :exit, reason -> {:error, {:supervisor_exit, reason}}
        :error, reason -> {:error, {:supervisor_error, reason}}
        reason -> {:error, {:unexpected_failure, reason}}
      end
    end
  end

  # PATCHED (fuu): start sequencer at max of log and materializer versions.
  # Sequencer versions are last_committed + elapsed monotonic microseconds, so a
  # later epoch can leave a materializer ahead of recovered logs. Resolvers must
  # boot at the same last_version or they queue the first system transaction
  # until versions that will never arrive, then time out.
  @spec max_committed_version(term(), map()) :: term()
  def max_committed_version(log_last_version, context) when is_map(context) do
    info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

    materializer_versions =
      context
      |> Map.get(:available_services, %{})
      |> Enum.map(fn {_id, service} -> materializer_current_version(service, info_fn) end)
      |> Enum.filter(&version_value?/1)

    [log_last_version | materializer_versions]
    |> Enum.filter(&version_value?/1)
    |> case do
      [] -> log_last_version
      versions -> Enum.max_by(versions, &version_rank/1)
    end
  end

  defp materializer_current_version(service, info_fn) do
    case materializer_ref(service) do
      nil ->
        nil

      ref ->
        case info_fn.(ref, [:current_version, :durable_version]) do
          {:ok, %{current_version: version}} when is_binary(version) or is_integer(version) ->
            version

          {:ok, %{durable_version: version}} when is_binary(version) or is_integer(version) ->
            version

          _other ->
            nil
        end
    end
  end

  defp materializer_ref({:materializer, name}) when not is_integer(name), do: name
  defp materializer_ref({:materializer, name, _shard_id}) when not is_integer(name), do: name
  defp materializer_ref({{:materializer, _shard_id}, name}), do: name
  defp materializer_ref(_service), do: nil

  defp default_materializer_info(ref, fact_names) do
    Worker.info(ref, fact_names, timeout_in_ms: 5_000)
  end

  defp version_value?(version) when is_integer(version) and version >= 0, do: true
  defp version_value?(version) when is_binary(version) and byte_size(version) == 8, do: true
  defp version_value?(_version), do: false

  defp version_rank(version) when is_integer(version), do: version
  defp version_rank(version) when is_binary(version) and byte_size(version) == 8, do: :binary.decode_unsigned(version)
end
