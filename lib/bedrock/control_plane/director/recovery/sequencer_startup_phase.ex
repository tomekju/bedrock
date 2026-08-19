defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase do
  @moduledoc """
  Solves the fundamental ordering problem by starting the sequencer component that
  provides authoritative global version numbers for all transactions.

  Without global ordering, transaction processing would collapse as different components
  assign conflicting version numbers to concurrent transactions. The sequencer is a
  critical singleton component where only one instance runs cluster-wide to ensure
  consistent version assignment.

  Starts the sequencer process on the director's current node with the last committed
  version from the version vector as the starting point. The sequencer assigns version
  numbers incrementally from this baseline, ensuring no gaps or overlaps in the sequence.
  Configured with current epoch, director PID, and OTP name for service coordination.

  Immediately halts recovery with a fatal error if sequencer startup fails, since version
  assignment is fundamental to transaction processing. Unlike temporary resource shortages,
  sequencer startup failure indicates serious system problems requiring immediate attention.

  Transitions to proxy startup once the sequencer is operational and ready.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.Service.Worker

  require Logger

  @doc """
  Execute the sequencer startup phase of recovery.

  Starts the sequencer component with the current epoch and last committed version
  from the version vector.
  """
  @impl true
  def execute(recovery_attempt, context) do
    starter_fn = get_starter_function(recovery_attempt, context)

    recovery_attempt
    |> build_sequencer_child_spec(context)
    |> starter_fn.(node())
    |> handle_sequencer_result(recovery_attempt)
  end

  # Private helper functions

  @spec get_starter_function(RecoveryAttempt.t(), map()) :: (Supervisor.child_spec(), node() ->
                                                               {:ok, pid()} | {:error, term()})
  defp get_starter_function(recovery_attempt, context) do
    Map.get(context, :start_supervised_fn, fn child_spec, _node ->
      starter_fn = :sup |> recovery_attempt.cluster.otp_name() |> Shared.starter_for()
      starter_fn.(child_spec, node())
    end)
  end

  @spec build_sequencer_child_spec(RecoveryAttempt.t(), map()) :: Supervisor.child_spec()
  defp build_sequencer_child_spec(recovery_attempt, context) do
    {_first_version, log_last_version} = recovery_attempt.version_vector
    last_committed_version = max_committed_version(log_last_version, context)

    if last_committed_version != log_last_version do
      Logger.info(
        "PATCHED (fuu): start sequencer at max of log and materializer versions; log=#{inspect(log_last_version)} sequencer=#{inspect(last_committed_version)}"
      )
    end

    Sequencer.child_spec(
      cluster: recovery_attempt.cluster,
      director: self(),
      epoch: recovery_attempt.epoch,
      last_committed_version: last_committed_version,
      otp_name: recovery_attempt.cluster.otp_name(:sequencer)
    )
  end

  # PATCHED (fuu): start sequencer at max of log and materializer versions.
  # Sequencer versions are last_committed + elapsed monotonic microseconds, so a
  # later epoch can leave a materializer ahead of recovered logs. Starting the
  # sequencer at the log last version then makes new commits too old for that
  # materializer.
  defp max_committed_version(log_last_version, context) do
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

  @spec handle_sequencer_result({:ok, pid()} | {:error, term()}, RecoveryAttempt.t()) ::
          {RecoveryAttempt.t(), module()} | {RecoveryAttempt.t(), {:stalled, term()}}
  defp handle_sequencer_result({:ok, sequencer}, recovery_attempt) do
    updated_recovery_attempt = %{recovery_attempt | sequencer: sequencer}

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase}
  end

  defp handle_sequencer_result({:error, reason}, recovery_attempt) do
    {recovery_attempt, {:error, {:failed_to_start, :sequencer, node(), reason}}}
  end
end
